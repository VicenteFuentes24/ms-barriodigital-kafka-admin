package cl.barriodigital.ms_kafka_admin;

import cl.barriodigital.ms_kafka_admin.config.AdminProperties;
import cl.barriodigital.ms_kafka_admin.service.TopicPolicy;
import cl.barriodigital.ms_kafka_admin.exception.AdminApiException;
import cl.barriodigital.ms_kafka_admin.security.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import static org.assertj.core.api.Assertions.*;

class TopicPolicyTest {
    final TopicPolicy policy=new TopicPolicy(new AdminProperties(true,Set.of("additional"),500));
    @ParameterizedTest @ValueSource(strings={"",".","..","bad topic","bad/name","ñ","bad\nname"})
    void rejectsInvalidNames(String name) { assertThatThrownBy(() -> policy.validateName(name)).isInstanceOf(AdminApiException.class); }
    @Test void rejectsTooLongName() { assertThatThrownBy(() -> policy.validateName("a".repeat(250))).isInstanceOf(AdminApiException.class); }
    @ParameterizedTest @ValueSource(strings={"bd.requests.events.v1","bd.requests.analytics.v1","__consumer_offsets","__anything","additional"})
    void protectedEvenWhenWritesEnabled(String name) { assertThatThrownBy(() -> policy.requireWrite(name)).isInstanceOf(AdminApiException.class); }
    @Test void disabledBlocksOtherwiseValidName() {
        assertThatThrownBy(() -> new TopicPolicy(new AdminProperties(false,Set.of(),500)).requireWrite("demo")).isInstanceOf(AdminApiException.class);
    }
    @Test void validatesAllowedConfigurations() {
        assertThat(policy.validateConfigs(Map.of("retention.ms","1000","min.insync.replicas","2","cleanup.policy","compact,delete"),3)).hasSize(3);
        for (Map<String,String> values : List.of(Map.of("retention.ms","-1"),Map.of("retention.ms","garbage"),Map.of("min.insync.replicas","4"),
                Map.of("cleanup.policy","drop"),Map.of("replication.factor","2"),Map.of("retention.ms","0"))) {
            assertThatThrownBy(() -> policy.validateConfigs(values,3)).isInstanceOf(AdminApiException.class);
        }
    }
    @Test void missingOrWeakCredentialsFailWithoutEchoingThem() {
        var credentials=new AdminCredentials();
        assertThatThrownBy(() -> new SecurityConfiguration().adminUsers(credentials)).isInstanceOf(IllegalStateException.class);
        credentials.setUsername("admin"); credentials.setPassword("short");
        assertThatThrownBy(credentials::requireConfigured).hasMessageNotContaining("short");
        credentials.setPassword(UUID.randomUUID().toString());
        assertThatCode(credentials::requireConfigured).doesNotThrowAnyException();
        assertThat(credentials.toString()).doesNotContain(credentials.getPassword());
    }
}
