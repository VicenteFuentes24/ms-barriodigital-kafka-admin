package cl.barriodigital.ms_kafka_admin.config;

import java.time.Duration;
import java.util.Set;
import jakarta.validation.constraints.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.kafka-admin")
public record AdminProperties(boolean writesEnabled, Set<String> protectedTopics,
        @Min(500) @Max(30000) int timeoutMs) {
    public Duration timeout() { return Duration.ofMillis(timeoutMs); }
}
