package cl.barriodigital.ms_kafka_admin.exception;

import org.springframework.http.HttpStatus;

public class AdminApiException extends RuntimeException {
    private final HttpStatus status;
    private final String code;
    public AdminApiException(HttpStatus status, String code, String message) {
        super(message); this.status = status; this.code = code;
    }
    public HttpStatus status() { return status; }
    public String code() { return code; }
    public static AdminApiException invalid(String message) { return new AdminApiException(HttpStatus.BAD_REQUEST,"INVALID_REQUEST",message); }
    public static AdminApiException forbidden(String message) { return new AdminApiException(HttpStatus.FORBIDDEN,"OPERATION_BLOCKED",message); }
}
