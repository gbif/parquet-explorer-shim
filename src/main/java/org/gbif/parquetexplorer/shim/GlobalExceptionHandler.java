package org.gbif.parquetexplorer.shim;

import org.gbif.parquetexplorer.shim.query.QueryBusyException;
import org.gbif.parquetexplorer.shim.query.QueryExecutionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadPath(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(QueryExecutionException.class)
    public ResponseEntity<String> handleQueryExecution(QueryExecutionException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(QueryBusyException.class)
    public ResponseEntity<String> handleQueryBusy(QueryBusyException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ex.getMessage());
    }
}
