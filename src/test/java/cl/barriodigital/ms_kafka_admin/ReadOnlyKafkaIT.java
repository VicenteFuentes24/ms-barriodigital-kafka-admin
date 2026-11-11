package cl.barriodigital.ms_kafka_admin;

import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;

/** Explicit opt-in. GET requests only; never creates, deletes, produces, consumes or commits offsets. */
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT, properties={
        "app.kafka-admin.writes-enabled=false","server.address=127.0.0.1",
        "logging.level.root=INFO","logging.level.org.springframework=INFO","debug=false"})
class ReadOnlyKafkaIT {
    static final String PASSWORD=UUID.randomUUID().toString();
    @DynamicPropertySource static void credentials(DynamicPropertyRegistry r) {
        r.add("app.admin-auth.username",() -> "readonly-validation"); r.add("app.admin-auth.password",() -> PASSWORD);
    }
    @Value("${local.server.port}") int port;
    final JsonMapper json=JsonMapper.builder().build();
    final Map<String,JsonNode> evidence=new LinkedHashMap<>();
    JsonNode get(HttpClient client, String path) throws Exception {
        var auth=Base64.getEncoder().encodeToString(("readonly-validation:"+PASSWORD).getBytes(StandardCharsets.UTF_8));
        var response=client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+path))
                .header("Authorization","Basic "+auth).timeout(Duration.ofSeconds(40)).GET().build(),HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("GET %s response %s",path,response.body()).isEqualTo(200);
        var body=json.readTree(response.body()); evidence.put(path,body); return body;
    }
    @Test void existingClusterAndReportGroupCanBeInspectedThroughAuthenticatedHttp() throws Exception {
        try (var client=HttpClient.newHttpClient()) {
            var base="/api/kafka/admin";
            assertThat(get(client,base+"/cluster").path("brokers").size()).isEqualTo(3);
            assertThat(get(client,base+"/brokers").size()).isEqualTo(3);
            get(client,base+"/topics");
            for (var entry : Map.of("bd.requests.events.v1","604800000","bd.requests.analytics.v1","86400000").entrySet()) {
                var topic=get(client,base+"/topics/"+entry.getKey());
                assertThat(topic.path("protectedTopic").asBoolean()).isTrue();
                assertThat(topic.path("partitions").size()).isEqualTo(3);
                for (var partition : topic.path("partitions")) {
                    assertThat(partition.path("replicas").size()).isEqualTo(3);
                    assertThat(partition.path("isr").size()).isEqualTo(3);
                }
                var config=get(client,base+"/topics/"+entry.getKey()+"/config");
                Map<String,String> values=new HashMap<>(); config.forEach(c -> values.put(c.path("name").asString(),c.path("value").asString()));
                assertThat(values).containsEntry("retention.ms",entry.getValue()).containsEntry("min.insync.replicas","2");
            }
            get(client,base+"/consumer-groups");
            get(client,base+"/consumer-groups/bd-report-v1");
            get(client,base+"/consumer-groups/bd-report-v1/offsets");
            get(client,base+"/consumer-groups/bd-report-v1/lag");
            assertThat(get(client,"/actuator/health").path("status").asString()).isEqualTo("UP");
            var anonymous=client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+base+"/brokers")).GET().build(),HttpResponse.BodyHandlers.ofString());
            assertThat(anonymous.statusCode()).isEqualTo(401);
        } finally {
            Files.writeString(Path.of("target/live-readonly-evidence.json"),json.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
        }
    }
}
