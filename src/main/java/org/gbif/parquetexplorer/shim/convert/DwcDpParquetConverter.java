package org.gbif.parquetexplorer.shim.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Batch CLI: converts a DWC-DP dataset's TSV/CSV resources to parquet.
 * ----------------------------------------------------------------------
 * Column types come from the resource's Frictionless Table Schema
 * (schema.fields[].type in datapackage.json) rather than being re-inferred
 * from the CSV content — DWC-DP already declares the type for every field,
 * so there's no reason to trust DuckDB's auto-detection over it.
 *
 * Run against the local NFS mount (local.base-path in application.yml),
 * not WebHDFS — writing parquet back out over WebHDFS's OPEN-only, no-Range
 * API would require dealing with its CREATE/APPEND semantics for no benefit
 * when the same files are already reachable as local paths.
 *
 * Usage:
 *   java -cp target/classes:<deps> org.gbif.parquetexplorer.shim.convert.DwcDpParquetConverter \
 *       <packageDir> [outputDir] [--force]
 *
 *   packageDir  - directory containing datapackage.json and its resource files
 *   outputDir   - defaults to <packageDir>/parquet
 *   --force     - reconvert even if an up-to-date parquet output already exists
 *
 * Output: one <resource>.parquet per converted resource, plus a sibling
 * datapackage.json with resource path/format/mediatype rewritten to point
 * at the parquet files (dialect is dropped since it no longer applies).
 */
public final class DwcDpParquetConverter {

    private static final Logger log = Logger.getLogger(DwcDpParquetConverter.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: DwcDpParquetConverter <packageDir> [outputDir] [--force]");
            System.exit(1);
        }
        Path packageDir = Path.of(args[0]).toAbsolutePath().normalize();
        boolean force = false;
        Path outputDir = null;
        for (int i = 1; i < args.length; i++) {
            if ("--force".equals(args[i])) {
                force = true;
            } else if (outputDir == null) {
                outputDir = Path.of(args[i]);
            }
        }
        if (outputDir == null) {
            outputDir = packageDir.resolve("parquet");
        }
        new DwcDpParquetConverter().convert(packageDir, outputDir, force);
    }

    public void convert(Path packageDir, Path outputDir, boolean force) throws Exception {
        Path descriptor = packageDir.resolve("datapackage.json");
        if (!Files.isRegularFile(descriptor)) {
            throw new IllegalArgumentException("No datapackage.json in " + packageDir);
        }

        ObjectNode dp = (ObjectNode) MAPPER.readTree(descriptor.toFile());
        JsonNode resourcesNode = dp.get("resources");
        if (resourcesNode == null || !resourcesNode.isArray()) {
            throw new IllegalArgumentException("datapackage.json has no 'resources' array: " + descriptor);
        }
        ArrayNode resources = (ArrayNode) resourcesNode;

        Files.createDirectories(outputDir);

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            int total = resources.size();
            int index = 0;
            for (JsonNode resourceNode : resources) {
                index++;
                convertResource(conn, packageDir, outputDir, (ObjectNode) resourceNode, force, index, total);
            }
        }

        Path outputDescriptor = outputDir.resolve("datapackage.json");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputDescriptor.toFile(), dp);
        log.info("Wrote " + outputDescriptor);
    }

    private void convertResource(Connection conn, Path packageDir, Path outputDir,
                                  ObjectNode resource, boolean force, int index, int total) throws SQLException {
        String name = resource.path("name").asText(null);
        String relativePath = resource.path("path").asText(null);
        if (name == null || relativePath == null) {
            log.warning("Skipping resource with missing name/path: " + resource);
            return;
        }

        String format = resource.path("format").asText("").toLowerCase();
        boolean isDelimited = format.equals("csv") || format.equals("tsv")
                || relativePath.endsWith(".csv") || relativePath.endsWith(".tsv");
        if (!isDelimited) {
            log.info("Skipping " + name + ": format '" + format + "' is not csv/tsv, leaving as-is.");
            return;
        }

        Path sourceFile = packageDir.resolve(relativePath);
        if (!Files.isRegularFile(sourceFile)) {
            log.warning("Skipping " + name + ": source file not found at " + sourceFile);
            return;
        }

        Path outFile = outputDir.resolve(name + ".parquet");
        if (!force && isUpToDate(sourceFile, outFile)) {
            log.info(name + ": " + outFile + " already up to date, skipping (use --force to reconvert).");
            rewriteResourceNode(resource, name);
            return;
        }

        JsonNode schema = resource.path("schema");
        JsonNode fields = schema.path("fields");
        if (!fields.isArray() || fields.isEmpty()) {
            log.warning("Skipping " + name + ": no schema.fields declared.");
            return;
        }

        log.info(String.format("Converting %s (%d/%d): %s -> %s", name, index, total,
                sourceFile.getFileName(), outFile.getFileName()));

        char delimiter = resolveDelimiter(resource, format);
        char quote = resolveQuote(resource);
        String castSelect = buildCastSelectClause(fields);
        String mismatchWhere = buildMismatchWhereClause(fields);

        // read_csv's explicit columns={...} map (the previous approach here,
        // needed to enforce our declared schema types instead of trusting
        // DuckDB's auto-detection) knocks DuckDB 0.10's CSV reader off its
        // parallel scan path onto a single-threaded one -- confirmed by timing
        // both against a real 3.1GB/15.7M-row DWC-DP survey resource:
        // auto_detect finished in ~1s, columns={...} was still running after
        // 10+ minutes. Worse, real DWC-DP data does violate its own declared
        // schema sometimes (that file's abundanceCap is declared 'integer' but
        // contains literal 'false' values), and the old ignore_errors/
        // rejects_table fallback for that case was column={...}-based too, so
        // it was just as slow -- confirmed separately still running after 5+
        // minutes on the same file.
        //
        // So: auto_detect drives the parallel scan, every value is read as
        // VARCHAR (all_varchar=true, so nothing can fail to parse), and
        // TRY_CAST enforces our declared types in the SELECT list. A value
        // that doesn't fit its declared type becomes NULL in that column
        // instead of discarding the whole row (unlike the old rejects_table,
        // which dropped every other valid column alongside the bad one) --
        // measured at the same ~1s/GB speed as a clean file, mismatches or not.
        String sql = buildCopySql(sourceFile, outFile, delimiter, quote, castSelect);
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }

        long rejectCount = mismatchWhere == null ? 0
                : countRows(conn, buildAllVarcharCsvExpr(sourceFile, delimiter, quote) + " WHERE " + mismatchWhere);
        if (rejectCount > 0) {
            log.warning(name + ": " + rejectCount + " row(s) have a value that didn't match its declared"
                    + " schema type and was set to NULL in the parquet output (row itself was kept).");
        }

        long rowCount = countRows(conn, "read_parquet(" + sqlLiteral(outFile.toString()) + ")");
        log.info(name + ": " + sourceFile.getFileName() + " -> " + outFile
                + " (" + rowCount + " rows" + (rejectCount > 0 ? ", " + rejectCount + " with a nulled value" : "") + ")");

        rewriteResourceNode(resource, name);
    }

    /**
     * auto_detect drives DuckDB's parallel CSV scan, all_varchar keeps every
     * value readable as text (so no row can fail to parse), and castSelect
     * (built by buildCastSelectClause, using TRY_CAST) enforces our declared
     * schema types without a manual columns={...} map, which forces DuckDB
     * 0.10's CSV reader onto a single-threaded path -- see convertResource.
     */
    private String buildCopySql(Path sourceFile, Path outFile, char delimiter, char quote, String castSelect) {
        return String.format(
                "COPY (SELECT %s FROM %s) TO %s (FORMAT PARQUET, COMPRESSION ZSTD)",
                castSelect,
                buildAllVarcharCsvExpr(sourceFile, delimiter, quote),
                sqlLiteral(outFile.toString()));
    }

    private String buildAllVarcharCsvExpr(Path sourceFile, char delimiter, char quote) {
        return String.format(
                "read_csv(%s, delim=%s, quote=%s, header=true, auto_detect=true, all_varchar=true, nullstr='')",
                sqlLiteral(sourceFile.toString()),
                sqlLiteral(String.valueOf(delimiter)),
                sqlLiteral(String.valueOf(quote)));
    }

    private boolean isUpToDate(Path sourceFile, Path outFile) {
        try {
            return Files.isRegularFile(outFile)
                    && Files.getLastModifiedTime(outFile).compareTo(Files.getLastModifiedTime(sourceFile)) >= 0;
        } catch (Exception e) {
            return false;
        }
    }

    private void rewriteResourceNode(ObjectNode resource, String name) {
        resource.put("path", name + ".parquet");
        resource.put("format", "parquet");
        resource.put("mediatype", "application/vnd.apache.parquet");
        resource.remove("dialect");
    }

    /**
     * TRY_CAST("field" AS TYPE) AS "field", ... -- enforces schema types on
     * top of an all_varchar read_csv, without CAST's failure mode of aborting
     * the whole COPY on one bad value (see convertResource).
     */
    private String buildCastSelectClause(JsonNode fields) {
        StringBuilder sb = new StringBuilder();
        Iterator<JsonNode> it = fields.elements();
        boolean first = true;
        while (it.hasNext()) {
            JsonNode field = it.next();
            String fieldName = field.path("name").asText();
            if (fieldName.isEmpty()) continue;
            String duckType = mapType(field.path("type").asText("string"));
            if (!first) sb.append(", ");
            String quotedName = sqlIdentifier(fieldName);
            sb.append("TRY_CAST(").append(quotedName).append(" AS ").append(duckType)
                    .append(") AS ").append(quotedName);
            first = false;
        }
        return sb.toString();
    }

    /**
     * ("field" IS NOT NULL AND TRY_CAST("field" AS TYPE) IS NULL) OR ... for
     * every non-VARCHAR field -- true for a row where TRY_CAST silently
     * nulled a value that didn't match its declared type. Returns null (no
     * query needed) if every field is VARCHAR, since TRY_CAST-to-VARCHAR from
     * already-text input can never fail.
     */
    private String buildMismatchWhereClause(JsonNode fields) {
        StringBuilder sb = new StringBuilder();
        Iterator<JsonNode> it = fields.elements();
        boolean first = true;
        while (it.hasNext()) {
            JsonNode field = it.next();
            String fieldName = field.path("name").asText();
            if (fieldName.isEmpty()) continue;
            String duckType = mapType(field.path("type").asText("string"));
            if ("VARCHAR".equals(duckType)) continue;
            String quotedName = sqlIdentifier(fieldName);
            if (!first) sb.append(" OR ");
            sb.append("(").append(quotedName).append(" IS NOT NULL AND TRY_CAST(")
                    .append(quotedName).append(" AS ").append(duckType).append(") IS NULL)");
            first = false;
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /**
     * Frictionless Table Schema type -> DuckDB SQL type. Real DWC-DP schemas
     * (checked against rs.gbif.org table-schemas) only use string/integer/number
     * in practice -- notably eventDate is declared 'string', not 'date', because
     * Darwin Core dates allow ISO 8601 intervals and partial dates that don't fit
     * a strict SQL DATE. date/datetime are mapped defensively in case a future
     * DWC-DP table or a non-DWC-DP package does use them.
     */
    private static String mapType(String frictionlessType) {
        return switch (frictionlessType) {
            case "integer", "year" -> "BIGINT";
            case "number" -> "DOUBLE";
            case "boolean" -> "BOOLEAN";
            case "date" -> "DATE";
            case "datetime" -> "TIMESTAMP";
            default -> "VARCHAR";
        };
    }

    private char resolveDelimiter(ObjectNode resource, String format) {
        JsonNode dialect = resource.get("dialect");
        if (dialect != null && dialect.hasNonNull("delimiter")) {
            String d = dialect.get("delimiter").asText();
            if (!d.isEmpty()) return d.charAt(0);
        }
        return format.equals("tsv") ? '\t' : ',';
    }

    private char resolveQuote(ObjectNode resource) {
        JsonNode dialect = resource.get("dialect");
        if (dialect != null && dialect.hasNonNull("quoteChar")) {
            String q = dialect.get("quoteChar").asText();
            if (!q.isEmpty()) return q.charAt(0);
        }
        return '"';
    }

    private long countRows(Connection conn, String tableOrFn) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tableOrFn)) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            return 0L;
        }
    }

    /** Single-quoted DuckDB SQL string literal with embedded quotes escaped. */
    private static String sqlLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** Double-quoted DuckDB SQL identifier with embedded quotes escaped. */
    private static String sqlIdentifier(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
