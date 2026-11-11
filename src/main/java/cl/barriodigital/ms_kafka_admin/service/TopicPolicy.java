package cl.barriodigital.ms_kafka_admin.service;

import java.util.*;
import cl.barriodigital.ms_kafka_admin.config.AdminProperties;
import cl.barriodigital.ms_kafka_admin.exception.AdminApiException;
import org.springframework.stereotype.Component;

@Component
public class TopicPolicy {
    private static final Set<String> CRITICAL = Set.of("bd.requests.events.v1","bd.requests.analytics.v1");
    private static final Set<String> ALLOWED_CONFIGS = Set.of("retention.ms","cleanup.policy","min.insync.replicas");
    private final AdminProperties properties;
    public TopicPolicy(AdminProperties properties) { this.properties = properties; }
    public void validateName(String name) {
        if (name == null || !name.matches("[a-zA-Z0-9._-]{1,249}") || name.equals(".") || name.equals(".."))
            throw AdminApiException.invalid("Topic name must contain 1-249 ASCII letters, digits, dots, underscores or hyphens, excluding . and ..");
    }
    public void validateGroup(String id) {
        if (id == null || id.isBlank() || id.length()>255 || id.chars().anyMatch(Character::isISOControl))
            throw AdminApiException.invalid("Invalid consumer group identifier");
    }
    public boolean protectedTopic(String name) {
        return name.startsWith("__") || CRITICAL.contains(name)
                || (properties.protectedTopics()!=null && properties.protectedTopics().contains(name));
    }
    public void requireWrite(String name) {
        validateName(name);
        if (!properties.writesEnabled()) throw AdminApiException.forbidden("Administrative writes are disabled");
        if (protectedTopic(name)) throw AdminApiException.forbidden("Protected and internal topics cannot be modified");
    }
    public Map<String,String> validateConfigs(Map<String,String> values, int replicas) {
        if (values == null || values.isEmpty()) throw AdminApiException.invalid("At least one allowed configuration is required");
        var validated = new TreeMap<String,String>();
        for (var entry : values.entrySet()) {
            String key=entry.getKey(), value=entry.getValue();
            if (!ALLOWED_CONFIGS.contains(key) || value==null || value.isBlank())
                throw AdminApiException.invalid("Allowed configurations: retention.ms, cleanup.policy, min.insync.replicas");
            if (key.equals("cleanup.policy")) {
                if (!Set.of("delete","compact","delete,compact","compact,delete").contains(value))
                    throw AdminApiException.invalid("Invalid cleanup.policy");
            } else {
                try {
                    long number=Long.parseLong(value);
                    if (number<1 || (key.equals("min.insync.replicas") && number>replicas)) throw new NumberFormatException();
                } catch (NumberFormatException ex) { throw AdminApiException.invalid("Retention must be positive and min.insync.replicas must be between 1 and replication factor"); }
            }
            validated.put(key,value);
        }
        return validated;
    }
}
