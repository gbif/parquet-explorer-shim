package org.gbif.parquetexplorer.shim.query;

import java.util.List;

/** JSON response shape for server-side DuckDB query execution. */
public record QueryResult(
        List<String> columns,
        List<List<Object>> rows,
        int rowCount,
        boolean truncated,
        long elapsedMs) {
}
