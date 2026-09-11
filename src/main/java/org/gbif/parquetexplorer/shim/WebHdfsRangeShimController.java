package org.gbif.parquetexplorer.shim;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.util.AntPathMatcher;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Range-request shim for DuckDB's httpfs extension <-> WebHDFS/HttpFS.
 * ------------------------------------------------------
 * Called internally only, by DuckDbQueryService's server-side DuckDB via
 * DuckDbQueryController's self-loopback (see shim.self-base-url) — not by
 * the frontend directly. httpfs reads remote parquet/CSV files via HTTP
 * Range headers — it asks for the footer first, then specific row groups.
 * WebHDFS does NOT speak Range headers; it uses its own `offset` and
 * `length` query parameters on the OPEN operation instead.
 *
 * What this does:
 *   1. Translates incoming `Range: bytes=start-end` into WebHDFS
 *      `op=OPEN&offset=start&length=(end-start+1)`.
 *   2. Answers HEAD requests with Content-Length + Accept-Ranges, which
 *      httpfs uses to decide whether range reads are possible at all —
 *      get this wrong and it silently falls back to downloading the
 *      entire file.
 *
 * Deploy this behind Varnish for caching of repeated byte ranges (footer
 * reads in particular are requested on every file open) if hot datapackages
 * get queried often enough for that to matter.
 *
 * Config (application.yml or env vars):
 *   hdfs.webhdfs-base-url:  e.g. http://namenode.internal:9870/webhdfs/v1
 *   hdfs.allowed-prefixes:  comma-separated path prefixes this shim may serve
 */
@RestController
@RequestMapping("/hdfs")
public class WebHdfsRangeShimController {

    private static final Logger log = Logger.getLogger(WebHdfsRangeShimController.class.getName());

    private final RestTemplate restTemplate;

    public WebHdfsRangeShimController() {
        // Large parquet row groups can be hundreds of MB; the read timeout must be
        // long enough to receive them fully. Without an explicit timeout the default
        // is 0 (infinite), which means a stalled DataNode silently hangs the thread.
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10_000);
        factory.setReadTimeout(120_000);
        restTemplate = new RestTemplate(factory);
    }

    @Value("${hdfs.webhdfs-base-url}")
    private String webhdfsBaseUrl; // e.g. http://namenode.internal:9870/webhdfs/v1

    @Value("${hdfs.allowed-prefixes:/data}")
    private String allowedPrefixesRaw; // comma-separated, e.g. "/data/gbif,/data/public"

    @Value("${shim.webhdfs-user:}")
    private String webhdfsUser; // optional user.name for WebHDFS if your cluster requires it

    @Value("${hdfs.namenoderpcaddress:}")
    private String namenoderpcaddress; // Stackable HA routing param; omit if unused

    private static final Pattern RANGE_PATTERN = Pattern.compile("bytes=(\\d*)-(\\d*)");

    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

    /** Pulls the part of the URL after /hdfs/, e.g. /hdfs/data/foo.parquet -> /data/foo.parquet */
    private String pathFromRequest(HttpServletRequest request) {
        String bestMatchPattern = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        String fullPath = request.getRequestURI();
        String extracted = PATH_MATCHER.extractPathWithinPattern(bestMatchPattern, fullPath);
        return "/" + extracted;
    }

    // --- HEAD: tell the client the file size and that ranges are supported ---
    @RequestMapping(value = "/**", method = RequestMethod.HEAD)
    public ResponseEntity<Void> head(HttpServletRequest request) {
        String hdfsPath = extractAndValidatePath(pathFromRequest(request));

        URI statusUri = UriComponentsBuilder
                .fromHttpUrl(webhdfsBaseUrl + hdfsPath)
                .queryParam("op", "GETFILESTATUS")
                .queryParamIfPresent("user.name", optionalUser())
                .queryParamIfPresent("namenoderpcaddress", optionalNamenodeRpcAddress())
                .build(true)
                .toUri();

        FileStatusResponse status;
        try {
            status = restTemplate.getForObject(statusUri, FileStatusResponse.class);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
        }

        if (status == null || status.fileStatus == null) {
            return ResponseEntity.notFound().build();
        }

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.ACCEPT_RANGES, "bytes");
        headers.setContentLength(status.fileStatus.length);
        headers.add(HttpHeaders.LAST_MODIFIED, String.valueOf(status.fileStatus.modificationTime));
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);

        return ResponseEntity.ok().headers(headers).build();
    }

    // --- GET: serve the requested byte range (or whole file if no Range header) ---
    @RequestMapping(value = "/**", method = RequestMethod.GET)
    public ResponseEntity<byte[]> get(
            HttpServletRequest request,
            @RequestHeader(value = HttpHeaders.RANGE, required = false) String rangeHeader) {

        String hdfsPath = extractAndValidatePath(pathFromRequest(request));

        Long offset = null;
        Long length = null;
        boolean isPartial = false;

        if (rangeHeader != null) {
            Matcher m = RANGE_PATTERN.matcher(rangeHeader);
            if (m.matches()) {
                String startStr = m.group(1);
                String endStr = m.group(2);
                if (!startStr.isEmpty()) {
                    offset = Long.parseLong(startStr);
                    if (!endStr.isEmpty()) {
                        long end = Long.parseLong(endStr);
                        length = end - offset + 1;
                    }
                    isPartial = true;
                }
            }
        }

        UriComponentsBuilder openUriBuilder = UriComponentsBuilder
                .fromHttpUrl(webhdfsBaseUrl + hdfsPath)
                .queryParam("op", "OPEN")
                .queryParamIfPresent("user.name", optionalUser())
                .queryParamIfPresent("namenoderpcaddress", optionalNamenodeRpcAddress());

        if (offset != null) {
            openUriBuilder.queryParam("offset", offset);
        }
        if (length != null) {
            openUriBuilder.queryParam("length", length);
        }

        URI openUri = openUriBuilder.build(true).toUri();

        ResponseEntity<byte[]> hdfsResponse;
        try {
            hdfsResponse = restTemplate.exchange(openUri, HttpMethod.GET, null, byte[].class);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
        }

        byte[] body = hdfsResponse.getBody();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        headers.add(HttpHeaders.ACCEPT_RANGES, "bytes");

        if (isPartial && body != null) {
            long actualLength = body.length;
            // If WebHDFS/DataNode returned fewer bytes than requested, passing those
            // bytes through causes a silent Snappy/Parquet decompression failure in
            // DuckDB-WASM because column chunk boundaries no longer align. Return
            // 502 instead so the caller sees a real error rather than corrupt data.
            if (length != null && actualLength < length) {
                log.warning(String.format(
                    "WebHDFS short read: requested %d bytes at offset %d for %s, got %d",
                    length, offset, hdfsPath, actualLength));
                return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
            }
            long rangeStart = offset;
            long rangeEnd = rangeStart + actualLength - 1;
            // Note: without a separate GETFILESTATUS call here we don't know
            // total file size for the Content-Range "*/total" — callers that
            // need it should HEAD first (DuckDB-WASM does exactly this).
            headers.add(HttpHeaders.CONTENT_RANGE, "bytes " + rangeStart + "-" + rangeEnd + "/*");
            headers.setContentLength(actualLength);
            return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).headers(headers).body(body);
        }

        headers.setContentLength(body == null ? 0 : body.length);
        return ResponseEntity.ok().headers(headers).body(body);
    }

    private java.util.Optional<String> optionalUser() {
        return (webhdfsUser == null || webhdfsUser.isBlank())
                ? java.util.Optional.empty()
                : java.util.Optional.of(webhdfsUser);
    }

    private java.util.Optional<String> optionalNamenodeRpcAddress() {
        return (namenoderpcaddress == null || namenoderpcaddress.isBlank())
                ? java.util.Optional.empty()
                : java.util.Optional.of(namenoderpcaddress);
    }

    /**
     * Validates the requested path is under one of the configured allowed
     * prefixes. This is the only access control this shim performs — pair
     * it with network-level restrictions (e.g. only Varnish/the public LB
     * can reach this service) and consider adding real authn/z before
     * exposing broadly, especially given GBIF's "public-ish" audience.
     */
    private String extractAndValidatePath(String hdfsPath) {
        if (!hdfsPath.startsWith("/")) {
            hdfsPath = "/" + hdfsPath;
        }
        String[] allowed = allowedPrefixesRaw.split(",");
        boolean ok = false;
        for (String prefix : allowed) {
            if (hdfsPath.startsWith(prefix.trim())) {
                ok = true;
                break;
            }
        }
        if (!ok) {
            throw new IllegalArgumentException("Path not in allowed prefixes: " + hdfsPath);
        }
        // Reject path traversal explicitly even though startsWith above
        // already constrains the prefix.
        if (hdfsPath.contains("..")) {
            throw new IllegalArgumentException("Invalid path: " + hdfsPath);
        }
        return hdfsPath;
    }

    // --- WebHDFS GETFILESTATUS response shape (subset) ---
    // WebHDFS returns PascalCase keys, so we map them explicitly.
    static class FileStatusResponse {
        @JsonProperty("FileStatus")
        public FileStatus fileStatus;
    }

    static class FileStatus {
        public long length;
        public long modificationTime;
    }
}
