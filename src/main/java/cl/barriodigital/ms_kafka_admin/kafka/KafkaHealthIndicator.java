package cl.barriodigital.ms_kafka_admin.kafka;

import org.springframework.boot.health.contributor.*;
import org.springframework.stereotype.Component;

@Component("kafkaCluster")
public class KafkaHealthIndicator implements HealthIndicator {
    private final KafkaGateway kafka;
    public KafkaHealthIndicator(KafkaGateway kafka) { this.kafka=kafka; }
    @Override public Health health() {
        try { return kafka.brokers().isEmpty() ? Health.down().build() : Health.up().build(); }
        catch (RuntimeException ex) { return Health.down().build(); }
    }
}
