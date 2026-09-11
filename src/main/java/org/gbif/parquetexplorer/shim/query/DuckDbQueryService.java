package org.gbif.parquetexplorer.shim.query;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Runs SQL against a datapackage's parquet/CSV resources using an in-process
 * DuckDB, on a small fixed pool of pre-warmed connections. This is the sole
 * query execution engine behind the datapackage explorer — every resource
 * in a datapackage becomes a named view in one call, so SQL can join across
 * them directly.
 *
 * Safety model: this DuckDB has real filesystem and network access, so
 * incoming SQL is restricted to read-only statements (validateReadOnly)
 * before it ever reaches a connection. Treat this endpoint as no less
 * sensitive than the WebHDFS shim it sits next to.
 */
@Service
public class DuckDbQueryService {

    private static final Logger log = Logger.getLogger(DuckDbQueryService.class.getName());

    private static final Pattern ALLOWED_START = Pattern.compile(
            "(?is)^\\s*(SELECT|WITH|DESCRIBE|SHOW|EXPLAIN|SUMMARIZE)\\b");

    private static final Pattern IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    // Defense in depth beyond the allowlist above — catches statements smuggled
    // in via a CTE or subquery that would otherwise pass the leading-keyword check.
    private static final Pattern DENYLIST = Pattern.compile(
            "(?is)\\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|COPY|ATTACH|DETACH|"
                    + "INSTALL|LOAD|EXPORT|IMPORT|SET|CALL|EXECUTE|VACUUM|CHECKPOINT|"
                    + "GRANT|REVOKE|TRUNCATE|PRAGMA)\\b");

    private final BlockingQueue<Connection> pool;
    private final int maxRows;
    private final int timeoutSeconds;
    private final int borrowTimeoutSeconds;

    public DuckDbQueryService(
            @Value("${duckdb.query.pool-size:4}") int poolSize,
            @Value("${duckdb.query.max-rows:10000}") int maxRows,
            @Value("${duckdb.query.timeout-seconds:60}") int timeoutSeconds,
            @Value("${duckdb.query.borrow-timeout-seconds:5}") int borrowTimeoutSeconds) throws SQLException {
        this.maxRows = maxRows;
        this.timeoutSeconds = timeoutSeconds;
        this.borrowTimeoutSeconds = borrowTimeoutSeconds;
        this.pool = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            pool.add(newConnection());
        }
    }

    private Connection newConnection() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        // httpfs is required for the /hdfs loopback source (see
        // DuckDbQueryController) and is a no-op extra for local reads.
        // INSTALL fetches from DuckDB's extension repository on first use and
        // caches it on disk — if this deployment's network egress is as
        // restricted as the Maven mirror note in pom.xml suggests, pre-seed
        // the extension cache out of band or this will fail at startup.
        try (Statement st = conn.createStatement()) {
            st.execute("INSTALL httpfs; LOAD httpfs;");
        } catch (SQLException e) {
            conn.close();
            throw e;
        }
        return conn;
    }

    /**
     * Runs sql against a set of named views, one per entry in viewSources —
     * viewSources.get("event") becomes a view named "event", readable via
     * FROM/JOIN in sql, backed by read_parquet/read_csv over the file(s) at
     * that entry's paths (multiple paths for a split/multi-part resource).
     * This is what makes cross-resource joins over a datapackage possible:
     * DuckDbQueryController builds one entry per resource.
     */
    public QueryResult queryMultiSource(Map<String, List<String>> viewSources, String sql) {
        validateReadOnly(sql);
        if (viewSources == null || viewSources.isEmpty()) {
            throw new IllegalArgumentException("At least one source is required");
        }
        for (String viewName : viewSources.keySet()) {
            if (!IDENTIFIER.matcher(viewName).matches()) {
                throw new IllegalArgumentException("Invalid view name: " + viewName);
            }
        }
        Connection conn = borrow();
        boolean healthy = true;
        try {
            for (Map.Entry<String, List<String>> entry : viewSources.entrySet()) {
                try (Statement viewStmt = conn.createStatement()) {
                    viewStmt.execute("CREATE OR REPLACE VIEW " + entry.getKey() + " AS SELECT * FROM "
                            + readExpr(entry.getValue()));
                }
            }
            try (Statement st = conn.createStatement()) {
                // Best-effort: duckdb_jdbc's setQueryTimeout support has historically
                // been partial (it may not interrupt a query already running natively),
                // so this is a safety net, not a guarantee.
                st.setQueryTimeout(timeoutSeconds);
                long start = System.currentTimeMillis();
                try (ResultSet rs = st.executeQuery(sql)) {
                    return collect(rs, start);
                }
            }
        } catch (SQLException e) {
            healthy = false;
            throw new QueryExecutionException(e.getMessage(), e);
        } finally {
            if (healthy) {
                pool.offer(conn);
            } else {
                replaceBroken(conn);
            }
        }
    }

    private Connection borrow() {
        try {
            Connection conn = pool.poll(borrowTimeoutSeconds, TimeUnit.SECONDS);
            if (conn == null) {
                throw new QueryBusyException(
                        "Server is busy running other queries — try again shortly.");
            }
            return conn;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryBusyException("Interrupted while waiting for a free connection.");
        }
    }

    private void replaceBroken(Connection broken) {
        try {
            broken.close();
        } catch (Exception ignore) {
            // already broken, nothing to do
        }
        try {
            pool.offer(newConnection());
        } catch (SQLException e) {
            // Pool permanently shrinks by one connection's worth of capacity;
            // acceptable degradation rather than crashing the request thread.
            log.log(Level.SEVERE, "Failed to replace broken DuckDB connection", e);
        }
    }

    @PreDestroy
    void shutdown() {
        Connection conn;
        while ((conn = pool.poll()) != null) {
            try {
                conn.close();
            } catch (SQLException ignore) {
                // shutting down anyway
            }
        }
    }

    /** Picks the right DuckDB reader function based on the source file's extension. */
    private String readExpr(List<String> sources) {
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("At least one source path is required");
        }
        String lower = sources.get(0).toLowerCase();
        // Single-path case reads as a plain literal; multi-path (split resources)
        // uses DuckDB's list-of-files form. Format is inferred from the first path
        // and assumed uniform across the list, matching how DwcDpParquetConverter
        // and the frontend both treat one resource as a single format.
        // header=true is explicit, not left to read_csv_auto's sniffer: DWC-DP data
        // (this tool's main CSV/TSV audience — see DwcDpParquetConverter) is
        // overwhelmingly all-VARCHAR, which gives the type-mismatch heuristic
        // read_csv_auto normally uses to detect a header row nothing to key off —
        // it silently treats the header line as a data row instead.
        if (sources.size() == 1) {
            String path = sources.get(0);
            if (lower.endsWith(".csv")) {
                return "read_csv(" + sqlLiteral(path) + ", auto_detect=true, header=true)";
            }
            if (lower.endsWith(".tsv")) {
                return "read_csv(" + sqlLiteral(path) + ", auto_detect=true, header=true, delim=E'\\t')";
            }
            return "read_parquet(" + sqlLiteral(path) + ")";
        }
        String fileList = sources.stream().map(DuckDbQueryService::sqlLiteral).collect(Collectors.joining(", "));
        if (lower.endsWith(".csv")) {
            return "read_csv([" + fileList + "], auto_detect=true, header=true)";
        }
        if (lower.endsWith(".tsv")) {
            return "read_csv([" + fileList + "], auto_detect=true, header=true, delim=E'\\t')";
        }
        return "read_parquet([" + fileList + "])";
    }

    private QueryResult collect(ResultSet rs, long startMs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        List<String> columns = new ArrayList<>(colCount);
        for (int i = 1; i <= colCount; i++) {
            columns.add(meta.getColumnLabel(i));
        }

        List<List<Object>> rows = new ArrayList<>();
        boolean truncated = false;
        while (rs.next()) {
            if (rows.size() >= maxRows) {
                truncated = true;
                break;
            }
            List<Object> row = new ArrayList<>(colCount);
            for (int i = 1; i <= colCount; i++) {
                row.add(toJsonSafe(rs.getObject(i)));
            }
            rows.add(row);
        }

        long elapsed = System.currentTimeMillis() - startMs;
        return new QueryResult(columns, rows, rows.size(), truncated, elapsed);
    }

    /** Passes JSON-native types through; stringifies everything else (BigDecimal, Timestamp, UUID, structs, …). */
    private Object toJsonSafe(Object value) {
        if (value == null
                || value instanceof String
                || value instanceof Boolean
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Double
                || value instanceof Float
                || value instanceof Short) {
            return value;
        }
        return String.valueOf(value);
    }

    private void validateReadOnly(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql must not be blank");
        }
        String trimmed = sql.trim();
        String body = trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
        if (body.contains(";")) {
            throw new IllegalArgumentException("Only a single SQL statement is allowed");
        }
        if (!ALLOWED_START.matcher(body).find()) {
            throw new IllegalArgumentException(
                    "Only read-only statements are allowed (SELECT/WITH/DESCRIBE/SHOW/EXPLAIN/SUMMARIZE)");
        }
        if (DENYLIST.matcher(body).find()) {
            throw new IllegalArgumentException("Statement contains a disallowed keyword");
        }
    }

    private static String sqlLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
