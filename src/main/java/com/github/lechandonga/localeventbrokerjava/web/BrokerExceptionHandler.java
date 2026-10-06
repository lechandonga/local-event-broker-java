package com.github.lechandonga.localeventbrokerjava.web;

import com.github.lechandonga.localeventbrokerjava.broker.CapacityExceededException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class BrokerExceptionHandler {

    /** 背压：容量已满时发布得到明确的 429（事件未被接受，可稍后重试）。 */
    @ExceptionHandler(CapacityExceededException.class)
    public ResponseEntity<Map<String, Object>> handleCapacity(CapacityExceededException ex) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of(
                "error", "CAPACITY_EXCEEDED",
                "message", ex.getMessage(),
                "time", Instant.now().toString()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "INVALID_REQUEST",
                "message", ex.getMessage(),
                "time", Instant.now().toString()));
    }
}
