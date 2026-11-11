package cl.barriodigital.ms_kafka_admin.exception;

import java.time.Instant;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.annotation.*;
import org.springframework.web.bind.MissingRequestHeaderException;

@RestControllerAdvice
public class ApiExceptionHandler {
    public record ApiError(int status, String code, String message, Instant timestamp) { }
    @ExceptionHandler(AdminApiException.class)
    ResponseEntity<ApiError> administrative(AdminApiException error) {
        return ResponseEntity.status(error.status()).body(new ApiError(error.status().value(),error.code(),error.getMessage(),Instant.now()));
    }
    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class,
            ConstraintViolationException.class, HandlerMethodValidationException.class,
            MethodArgumentTypeMismatchException.class, MissingRequestHeaderException.class})
    ResponseEntity<ApiError> invalid(Exception error) {
        return ResponseEntity.badRequest().body(new ApiError(400,"INVALID_REQUEST","Invalid or missing request fields",Instant.now()));
    }
    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiError> unexpected(Exception error) {
        LoggerFactory.getLogger(getClass()).error("admin_request_failed category={}", error.getClass().getSimpleName());
        return ResponseEntity.internalServerError().body(new ApiError(500,"INTERNAL_ERROR","Unable to complete administrative request",Instant.now()));
    }
}
