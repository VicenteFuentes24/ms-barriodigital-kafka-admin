package cl.barriodigital.ms_kafka_admin;

import cl.barriodigital.ms_kafka_admin.kafka.KafkaGateway;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.*;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.*;

@SpringBootTest(properties={"app.kafka-admin.writes-enabled=false"})
@AutoConfigureMockMvc
class WritesDisabledHttpTest {
    @DynamicPropertySource static void credentials(DynamicPropertyRegistry r) {
        r.add("app.admin-auth.username",() -> "test-admin"); r.add("app.admin-auth.password",UUID.randomUUID()::toString);
    }
    @Autowired MockMvc mvc;
    @MockitoBean Admin admin;
    @MockitoBean KafkaGateway kafka;
    @Test void everyWriteIsBlockedBeforeKafka() throws Exception {
        var base=MsKafkaAdminApplicationTests.BASE;
        for (var request : java.util.List.of(post(base+"/topics").content(MsKafkaAdminApplicationTests.CREATE),
                delete(base+"/topics/demo").header("X-Confirm-Topic","demo"),
                patch(base+"/topics/demo/config").content("{\"values\":{\"retention.ms\":\"1000\"}}"),
                post(base+"/topics/demo/partitions").content("{\"totalPartitions\":4}"))) {
            mvc.perform(request.contentType(MediaType.APPLICATION_JSON).with(user("admin").roles("ADMIN")).with(csrf())).andExpect(status().isForbidden());
        }
        verifyNoInteractions(kafka);
    }
    @Test void readsStillWork() throws Exception {
        when(kafka.topics()).thenReturn(java.util.List.of());
        mvc.perform(get(MsKafkaAdminApplicationTests.BASE+"/topics").with(user("admin").roles("ADMIN"))).andExpect(status().isOk());
    }
}
