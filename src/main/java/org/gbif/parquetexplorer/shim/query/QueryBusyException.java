package org.gbif.parquetexplorer.shim.query;

/** Thrown when every pooled DuckDB connection is busy and the borrow timeout elapses. */
public class QueryBusyException extends RuntimeException {
    public QueryBusyException(String message) {
        super(message);
    }
}
