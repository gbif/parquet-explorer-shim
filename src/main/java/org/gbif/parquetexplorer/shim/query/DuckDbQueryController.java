package org.gbif.parquetexplorer.shim.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Single query endpoint for the datapackage explorer: takes a GBIF dataset
 * UUID and crawl attempt number, and runs read-only SQL against every
 * resource in that attempt's datapackage at once (one DuckDB view per
 * resource, so SQL can join across them directly — e.g. "FROM event JOIN
 * occurrence ON ..."). Callers never see or choose a filesystem/HDFS path,
 * only the UUID and attempt — callers are expected to resolve which attempt
 * to use themselves (e.g. from GBIF's ingestion-history API), since this
 * endpoint has no opinion on "latest" and does not validate that the
 * attempt actually completed successfully, only that a datapackage exists
 * at that path.
 *
 *   POST /dataset/{uuid}/dwcdp/{attempt}/query   { "sql": "..." }
 *   POST /dataset/{uuid}/dwcdp/query              { "sql": "..." }
 *     (same as above, but resolves {attempt} itself to the dataset's last
 *     successful crawl attempt via GBIF's pipelines history API, so callers
 *     that just want "the latest good data" don't have to resolve it
 *     themselves first)
 *
 * Two repositories are checked, in order:
 *   1. Local: local.base-path + "/" + uuid + "/" + attempt, read directly
 *      off disk — no network hop, used whenever the dataset has been
 *      converted and lives there.
 *   2. HDFS: hdfs.dwcdp-root-path + "/" + uuid + "/" + attempt, read via
 *      this shim's own /hdfs endpoint over httpfs (WebHdfsRangeShimController's
 *      self-loopback, shim.self-base-url) — used only when the local
 *      repository doesn't have that dataset/attempt.
 * The two repositories are independent copies (e.g. a fast local cache vs.
 * the full HDFS archive) — this endpoint doesn't merge them, it just tries
 * local first and falls back whole-package to HDFS if local doesn't have it.
 *
 * SQL is restricted to read-only statements — see DuckDbQueryService.
 */
@RestController
public class DuckDbQueryController {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Used only to fetch datapackage.json through our own /hdfs loopback (see
    // buildHdfsDatapackageViews) — separate from the RestTemplate
    // WebHdfsRangeShimController uses to talk to WebHDFS itself; this client
    // only ever calls this same service.
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final DuckDbQueryService queryService;

    public DuckDbQueryController(DuckDbQueryService queryService) {
        this.queryService = queryService;
    }

    @Value("${local.base-path}")
    private String localBasePath;

    @Value("${hdfs.allowed-prefixes:/data}")
    private String hdfsAllowedPrefixesRaw;

    @Value("${hdfs.dwcdp-root-path:}")
    private String dwcdpRootPath;

    @Value("${shim.self-base-url:http://localhost:8080}")
    private String selfBaseUrl;

    // Used only for cheap WebHDFS metadata calls (GETFILESTATUS/LISTSTATUS) to
    // detect and expand directory-shaped resources — see hdfsResourceUrls.
    // Talked to directly rather than through the /hdfs loopback, since that
    // loopback (WebHdfsRangeShimController) exists specifically to translate
    // byte-range OPEN reads, not to proxy arbitrary WebHDFS operations.
    @Value("${hdfs.webhdfs-base-url:}")
    private String webhdfsBaseUrl;

    @Value("${shim.webhdfs-user:}")
    private String webhdfsUser;

    @Value("${hdfs.namenoderpcaddress:}")
    private String namenoderpcAddress;

    // Base URL of GBIF's own registry/pipelines API — same service the
    // frontend calls directly as VITE_GBIF_API_BASE_URL, used here
    // server-side only to resolve "last successful attempt" (see
    // fetchLastSuccessfulAttempt) for the attempt-less query endpoint below.
    @Value("${gbif.api-base-url:https://api.gbif.org/v1}")
    private String gbifApiBaseUrl;

    public record DatasetQueryRequest(String sql) {
    }

    @PostMapping("/dataset/{uuid}/dwcdp/{attempt}/query")
    public ResponseEntity<QueryResult> queryDatasetDwcDp(
            @PathVariable String uuid,
            @PathVariable String attempt,
            @RequestBody DatasetQueryRequest request) {
        String canonicalUuid = parseUuid(uuid);

        // A crawl attempt number is always a positive integer assigned by the
        // crawler; parsing it this way also rules out "..", "/", or other
        // traversal-relevant characters before it becomes part of a path.
        int attemptNumber;
        try {
            attemptNumber = Integer.parseInt(attempt);
            if (attemptNumber <= 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Not a valid crawl attempt number: " + attempt);
        }

        return runQuery(canonicalUuid, attemptNumber, request);
    }

    /**
     * Same as {@link #queryDatasetDwcDp}, but resolves the crawl attempt
     * itself via GBIF's pipelines history API instead of taking it as a path
     * parameter — for callers that just want the dataset's latest
     * successfully-crawled data and would otherwise have to look that
     * attempt number up themselves first.
     */
    @PostMapping("/dataset/{uuid}/dwcdp/query")
    public ResponseEntity<QueryResult> queryDatasetDwcDpLastSuccessful(
            @PathVariable String uuid,
            @RequestBody DatasetQueryRequest request) {
        String canonicalUuid = parseUuid(uuid);
        int attemptNumber = fetchLastSuccessfulAttempt(canonicalUuid);
        return runQuery(canonicalUuid, attemptNumber, request);
    }

    // UUID.toString() always returns the canonical lowercase form, so this
    // also normalizes case before it becomes part of a path. A canonical
    // UUID can never contain "..", "/", or other traversal-relevant
    // characters, so paths built from it need no further validation.
    private static String parseUuid(String uuid) {
        try {
            return UUID.fromString(uuid).toString();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Not a valid dataset UUID: " + uuid);
        }
    }

    /**
     * Calls GBIF's pipelines history API to find the last crawl attempt that
     * completed successfully for a dataset. Unlike every other GBIF API call
     * in this class, this endpoint's response body is a bare integer (e.g.
     * "42"), not JSON, so it's read as plain text rather than parsed with
     * the Jackson mapper.
     */
    private int fetchLastSuccessfulAttempt(String canonicalUuid) {
        String url = trimTrailingSlash(gbifApiBaseUrl) + "/pipelines/history/" + canonicalUuid + "/lastSuccessfulAttempt";
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new IllegalArgumentException("GET " + url + " -> HTTP " + resp.statusCode());
            }
            return Integer.parseInt(resp.body().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("GET " + url + " did not return a valid attempt number: " + e.getMessage());
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to resolve last successful attempt for " + canonicalUuid + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Interrupted resolving last successful attempt for " + canonicalUuid);
        }
    }

    /** Resolves the datapackage (local first, then HDFS) for one dataset/attempt and runs the query against it. */
    private ResponseEntity<QueryResult> runQuery(String canonicalUuid, int attemptNumber, DatasetQueryRequest request) {
        Path localPackageDir = Path.of(localBasePath).resolve(canonicalUuid).resolve(String.valueOf(attemptNumber)).normalize();
        Map<String, List<String>> views;
        if (Files.isRegularFile(localPackageDir.resolve("datapackage.json"))) {
            views = buildLocalDatapackageViews(localPackageDir);
        } else {
            if (dwcdpRootPath == null || dwcdpRootPath.isBlank()) {
                throw new IllegalArgumentException(
                        "Dataset " + canonicalUuid + " attempt " + attemptNumber
                                + " not found in the local repository, "
                                + "and hdfs.dwcdp-root-path is not configured to check HDFS");
            }
            String hdfsPackagePath = validateHdfsPath(
                    trimTrailingSlash(dwcdpRootPath) + "/" + canonicalUuid + "/" + attemptNumber + "/datapackage");
            views = buildHdfsDatapackageViews(hdfsPackagePath);
        }

        QueryResult result = queryService.queryMultiSource(views, request.sql());
        return ResponseEntity.ok(result);
    }

    /** Reads datapackage.json directly off disk and builds one view entry per resource. */
    private Map<String, List<String>> buildLocalDatapackageViews(Path packageDir) {
        Path descriptor = packageDir.resolve("datapackage.json");
        Map<String, List<String>> views = new LinkedHashMap<>();
        try {
            JsonNode dp = MAPPER.readTree(descriptor.toFile());
            for (JsonNode resource : dp.path("resources")) {
                String name = resource.path("name").asText(null);
                JsonNode pathNode = resource.path("path");
                if (name == null || pathNode.isMissingNode() || pathNode.isNull()) {
                    continue;
                }
                List<String> resourcePaths = new ArrayList<>();
                if (pathNode.isArray()) {
                    for (JsonNode p : pathNode) {
                        resourcePaths.addAll(resolveResourceFiles(packageDir, p.asText()));
                    }
                } else {
                    resourcePaths.addAll(resolveResourceFiles(packageDir, pathNode.asText()));
                }
                if (!resourcePaths.isEmpty()) {
                    views.put(sanitizeIdentifier(name), resourcePaths);
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read " + descriptor + ": " + e.getMessage());
        }

        if (views.isEmpty()) {
            throw new IllegalArgumentException("No usable resources found in " + descriptor);
        }
        return views;
    }

    /**
     * Fetches datapackage.json for an already-validated HDFS package directory
     * (via the /hdfs loopback) and builds one view entry per resource.
     */
    private Map<String, List<String>> buildHdfsDatapackageViews(String hdfsPackagePath) {
        String descriptorHdfsPath = hdfsResourcePath(hdfsPackagePath, "datapackage.json");
        String descriptorUrl = trimTrailingSlash(selfBaseUrl) + "/hdfs" + descriptorHdfsPath;

        Map<String, List<String>> views = new LinkedHashMap<>();
        try {
            JsonNode dp = fetchJson(descriptorUrl);
            for (JsonNode resource : dp.path("resources")) {
                String name = resource.path("name").asText(null);
                JsonNode pathNode = resource.path("path");
                if (name == null || pathNode.isMissingNode() || pathNode.isNull()) {
                    continue;
                }
                List<String> resourceUrls = new ArrayList<>();
                if (pathNode.isArray()) {
                    for (JsonNode p : pathNode) {
                        resourceUrls.addAll(hdfsResourceUrls(hdfsPackagePath, p.asText()));
                    }
                } else {
                    resourceUrls.addAll(hdfsResourceUrls(hdfsPackagePath, pathNode.asText()));
                }
                if (!resourceUrls.isEmpty()) {
                    views.put(sanitizeIdentifier(name), resourceUrls);
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read " + descriptorUrl + ": " + e.getMessage());
        }

        if (views.isEmpty()) {
            throw new IllegalArgumentException("No usable resources found in datapackage at " + hdfsPackagePath);
        }
        return views;
    }

    /** Joins a resource's relative path onto an already-validated HDFS package dir, re-validating the result. */
    private String hdfsResourcePath(String hdfsPackagePath, String resourcePath) {
        if (resourcePath == null || resourcePath.isBlank()) {
            throw new IllegalArgumentException("Resource has a blank path");
        }
        String combined = trimTrailingSlash(hdfsPackagePath) + "/" + resourcePath.replaceFirst("^/", "");
        // Re-runs the ".."/allowed-prefixes checks on the combined path — a resource whose
        // path tries to climb out of the package directory is rejected the same way a
        // top-level request would be, not just trusted because the package dir was already OK.
        return validateHdfsPath(combined);
    }

    /**
     * Resolves a resource's relative path within an already-validated HDFS
     * package dir into one or more source URLs for DuckDB. Most resources
     * are a single file, but this pipeline's Spark-based ingestion writes
     * each resource as a directory of part-files (Spark's standard
     * DataFrameWriter output: _SUCCESS + part-NNNNN-*.parquet) rather than a
     * single file — WebHDFS OPEN on a directory fails outright, so a
     * directory-shaped resource is expanded to its part-files here instead
     * of being read as if it were one file.
     */
    private List<String> hdfsResourceUrls(String hdfsPackagePath, String resourcePath) {
        String path = hdfsResourcePath(hdfsPackagePath, resourcePath);
        if (isHdfsDirectory(path)) {
            return listHdfsParquetPartFiles(path).stream()
                    .map(partPath -> trimTrailingSlash(selfBaseUrl) + "/hdfs" + partPath)
                    .collect(Collectors.toList());
        }
        return List.of(trimTrailingSlash(selfBaseUrl) + "/hdfs" + path);
    }

    /** GETFILESTATUS on an already-validated HDFS path; false (treat as file) if the check itself fails. */
    private boolean isHdfsDirectory(String hdfsPath) {
        if (webhdfsBaseUrl == null || webhdfsBaseUrl.isBlank()) {
            return false;
        }
        try {
            JsonNode status = fetchJson(webHdfsMetadataUrl(hdfsPath, "GETFILESTATUS"));
            return "DIRECTORY".equals(status.path("FileStatus").path("type").asText(""));
        } catch (IOException e) {
            // Can't determine — fall through as if it were a file; the OPEN
            // read that follows will fail with a clear error if that's wrong.
            return false;
        }
    }

    /** LISTSTATUS on an already-validated HDFS directory, filtered to its *.parquet part-files, sorted for determinism. */
    private List<String> listHdfsParquetPartFiles(String hdfsDirPath) {
        try {
            JsonNode listing = fetchJson(webHdfsMetadataUrl(hdfsDirPath, "LISTSTATUS"));
            List<String> partFiles = new ArrayList<>();
            for (JsonNode entry : listing.path("FileStatuses").path("FileStatus")) {
                if (!"FILE".equals(entry.path("type").asText(""))) {
                    continue;
                }
                String name = entry.path("pathSuffix").asText("");
                if (name.endsWith(".parquet")) {
                    partFiles.add(validateHdfsPath(trimTrailingSlash(hdfsDirPath) + "/" + name));
                }
            }
            if (partFiles.isEmpty()) {
                throw new IllegalArgumentException("No parquet part-files found under " + hdfsDirPath);
            }
            Collections.sort(partFiles);
            return partFiles;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to list HDFS directory " + hdfsDirPath + ": " + e.getMessage());
        }
    }

    /**
     * Talks to WebHDFS directly (not via the /hdfs loopback) for metadata
     * ops — GETFILESTATUS/LISTSTATUS return small JSON, unlike the
     * byte-range file reads the loopback exists to translate.
     */
    private String webHdfsMetadataUrl(String hdfsPath, String op) {
        StringBuilder url = new StringBuilder(trimTrailingSlash(webhdfsBaseUrl)).append(hdfsPath)
                .append("?op=").append(op);
        if (webhdfsUser != null && !webhdfsUser.isBlank()) {
            url.append("&user.name=").append(URLEncoder.encode(webhdfsUser, StandardCharsets.UTF_8));
        }
        if (namenoderpcAddress != null && !namenoderpcAddress.isBlank()) {
            url.append("&namenoderpcaddress=").append(URLEncoder.encode(namenoderpcAddress, StandardCharsets.UTF_8));
        }
        return url.toString();
    }

    /** Fetches and parses JSON through our own /hdfs loopback — used for datapackage.json on HDFS. */
    private JsonNode fetchJson(String url) throws IOException {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new IOException("GET " + url + " -> HTTP " + resp.statusCode());
            }
            return MAPPER.readTree(resp.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted fetching " + url, e);
        }
    }

    /**
     * Resolves a resource's path field relative to its already-validated
     * local package directory, guarding against a resource.path that tries
     * to climb out of it. Usually resolves to a single file, but also
     * handles a directory-shaped resource — the same Spark DataFrameWriter
     * layout (_SUCCESS + part-NNNNN-*.parquet) handled on the HDFS side by
     * hdfsResourceUrls, which shows up here too whenever local.base-path is
     * a verbatim mirror (e.g. an NFS sync) of that HDFS output rather than
     * DwcDpParquetConverter's single-file-per-resource output.
     */
    private List<String> resolveResourceFiles(Path packageDir, String resourcePath) {
        if (resourcePath == null || resourcePath.isBlank()) {
            throw new IllegalArgumentException("Resource has a blank path");
        }
        Path resolved = packageDir.resolve(resourcePath).normalize();
        if (!resolved.startsWith(packageDir)) {
            throw new IllegalArgumentException("Resource path escapes package directory: " + resourcePath);
        }
        if (Files.isRegularFile(resolved)) {
            return List.of(resolved.toString());
        }
        if (Files.isDirectory(resolved)) {
            try (Stream<Path> children = Files.list(resolved)) {
                List<String> partFiles = children
                        .filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".parquet"))
                        .map(Path::toString)
                        .sorted()
                        .collect(Collectors.toList());
                if (partFiles.isEmpty()) {
                    throw new IllegalArgumentException("No parquet part-files found under " + resolved);
                }
                return partFiles;
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to list " + resolved + ": " + e.getMessage());
            }
        }
        throw new IllegalArgumentException("Resource file not found: " + resolved);
    }

    /** DuckDB view names must be valid identifiers; resource names come from file content, not our config. */
    private static String sanitizeIdentifier(String name) {
        String cleaned = name.replaceAll("[^a-zA-Z0-9_]", "_");
        if (cleaned.isEmpty() || Character.isDigit(cleaned.charAt(0))) {
            cleaned = "t_" + cleaned;
        }
        return cleaned;
    }

    /** Mirrors WebHdfsRangeShimController#extractAndValidatePath. */
    private String validateHdfsPath(String inputPath) {
        if (inputPath == null || inputPath.isBlank()) {
            throw new IllegalArgumentException("path must not be blank");
        }
        String hdfsPath = inputPath.startsWith("/") ? inputPath : "/" + inputPath;
        if (hdfsPath.contains("..")) {
            throw new IllegalArgumentException("Invalid path: " + hdfsPath);
        }
        for (String prefix : hdfsAllowedPrefixesRaw.split(",")) {
            if (hdfsPath.startsWith(prefix.trim())) {
                return hdfsPath;
            }
        }
        throw new IllegalArgumentException("Path not in allowed prefixes: " + hdfsPath);
    }

    private static String trimTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
