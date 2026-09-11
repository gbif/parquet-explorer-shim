package org.gbif.parquetexplorer.shim.query;

/** Wraps a DuckDB SQLException so the controller layer can map it to an HTTP response. */
public class QueryExecutionException extends RuntimeException {
    public QueryExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
