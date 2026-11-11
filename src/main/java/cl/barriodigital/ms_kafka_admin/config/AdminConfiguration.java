package cl.barriodigital.ms_kafka_admin.config;

import java.util.HashMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;

@Configuration
@EnableConfigurationProperties({AdminProperties.class, KafkaProperties.class})
public class AdminConfiguration {
    @Bean(destroyMethod = "close")
    Admin kafkaAdminClient(KafkaProperties kafka, AdminProperties properties) {
        // Includes external TLS/SASL properties; no topic creation or bootstrap mutations.
        var config = new HashMap<String, Object>(kafka.buildAdminProperties());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, properties.timeoutMs());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(3000, properties.timeoutMs()));
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "bd-kafka-admin");
        return Admin.create(config);
    }
}
