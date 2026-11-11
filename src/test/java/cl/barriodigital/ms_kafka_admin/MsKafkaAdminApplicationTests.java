package cl.barriodigital.ms_kafka_admin;

import cl.barriodigital.ms_kafka_admin.exception.AdminApiException;
import cl.barriodigital.ms_kafka_admin.kafka.KafkaGateway;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.*;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.*;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.*;

@SpringBootTest(properties={"app.kafka-admin.writes-enabled=true"})
@AutoConfigureMockMvc
class MsKafkaAdminApplicationTests {
    static final String PASSWORD=UUID.randomUUID().toString();
    @DynamicPropertySource static void credentials(DynamicPropertyRegistry r) {
        r.add("app.admin-auth.username",() -> "test-admin"); r.add("app.admin-auth.password",() -> PASSWORD);
    }
    @Autowired MockMvc mvc;
    @MockitoBean Admin admin;
    @MockitoBean KafkaGateway kafka;
    static final String BASE="/api/kafka/admin";
    static final String CREATE="{\"name\":\"demo\",\"partitions\":3,\"replicationFactor\":3,\"retentionMs\":60000,\"cleanupPolicy\":\"delete\",\"minInsyncReplicas\":2}";
    @BeforeEach void setup() { when(kafka.brokers()).thenReturn(List.of(new Node(1,"one",9092),new Node(2,"two",9092),new Node(3,"three",9092))); }
    @Test void requiresAuthentication() throws Exception { mvc.perform(get(BASE+"/brokers")).andExpect(status().isUnauthorized()).andExpect(jsonPath("$.code").value("UNAUTHENTICATED")); }
    @Test void rejectsWrongPassword() throws Exception { mvc.perform(get(BASE+"/brokers").with(httpBasic("test-admin","wrong"))).andExpect(status().isUnauthorized()); }
    @Test void requiresAdministratorRole() throws Exception { mvc.perform(get(BASE+"/brokers").with(user("reader").roles("USER"))).andExpect(status().isForbidden()); }
    @Test void basicAuthenticatedReadReturnsBrokerData() throws Exception {
        mvc.perform(get(BASE+"/brokers").with(httpBasic("test-admin",PASSWORD))).andExpect(status().isOk()).andExpect(jsonPath("$[0].id").value(1));
    }
    @Test void healthIsPublicAndMinimal() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.components").doesNotExist()).andExpect(jsonPath("$.details").doesNotExist());
    }
    @Test void csrfIsRequiredForWrites() throws Exception {
        mvc.perform(post(BASE+"/topics").with(user("admin").roles("ADMIN")).contentType(MediaType.APPLICATION_JSON).content(CREATE)).andExpect(status().isForbidden());
        verify(kafka,never()).create(anyString(),anyInt(),anyShort(),anyMap());
    }
    @Test void realBasicAndSessionCsrfFlowCanCreate() throws Exception {
        var response=mvc.perform(get(BASE+"/csrf").with(httpBasic("test-admin",PASSWORD))).andExpect(status().isOk()).andReturn();
        var csrf=JsonMapper.builder().build().readTree(response.getResponse().getContentAsString());
        mvc.perform(post(BASE+"/topics").with(httpBasic("test-admin",PASSWORD)).session((MockHttpSession)response.getRequest().getSession(false))
                .header(csrf.get("headerName").asString(),csrf.get("token").asString()).contentType(MediaType.APPLICATION_JSON).content(CREATE))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.status").value("ACKNOWLEDGED"));
        verify(kafka).create(eq("demo"),eq(3),eq((short)3),anyMap());
    }
    @Test void validatesPayloadAndRejectsUnknownFields() throws Exception {
        for (String json : List.of("{}",CREATE.replace("\"partitions\":3","\"partitions\":0"),CREATE.replace("\"name\":\"demo\"","\"name\":\"bad name\""),CREATE.replace("60000","-1"),CREATE.replace("{","{\"unexpected\":1,"))) {
            mvc.perform(post(BASE+"/topics").with(user("admin").roles("ADMIN")).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(json)).andExpect(status().isBadRequest());
        }
        verify(kafka,never()).create(anyString(),anyInt(),anyShort(),anyMap());
    }
    @Test void protectsCriticalTopicsEvenWithCsrfAndAdmin() throws Exception {
        mvc.perform(delete(BASE+"/topics/bd.requests.events.v1").header("X-Confirm-Topic","bd.requests.events.v1").with(user("admin").roles("ADMIN")).with(csrf())).andExpect(status().isForbidden());
        verify(kafka,never()).delete(anyString());
    }
    @Test void deletionRequiresConfirmationAndReturnsAccepted() throws Exception {
        when(kafka.topic("demo")).thenReturn(new TopicDescription("demo",false,List.of()));
        mvc.perform(delete(BASE+"/topics/demo").with(user("admin").roles("ADMIN")).with(csrf())).andExpect(status().isForbidden());
        mvc.perform(delete(BASE+"/topics/demo").header("X-Confirm-Topic","demo").with(user("admin").roles("ADMIN")).with(csrf())).andExpect(status().isAccepted());
        verify(kafka,times(1)).delete("demo");
    }
    @Test void maps404409503AndDoesNotExposeTraces() throws Exception {
        for (HttpStatus responseStatus : List.of(HttpStatus.NOT_FOUND,HttpStatus.CONFLICT,HttpStatus.SERVICE_UNAVAILABLE)) {
            doThrow(new AdminApiException(responseStatus,"TEST_ERROR","Safe description")).when(kafka).topic("demo");
            mvc.perform(get(BASE+"/topics/demo").with(user("admin").roles("ADMIN"))).andExpect(status().is(responseStatus.value()))
                    .andExpect(jsonPath("$.code").value("TEST_ERROR")).andExpect(jsonPath("$.trace").doesNotExist());
        }
    }
    @Test void unexpectedFailureDoesNotExposeInternalMessage() throws Exception {
        when(kafka.topics()).thenThrow(new IllegalStateException("private broker details"));
        var result=mvc.perform(get(BASE+"/topics").with(user("admin").roles("ADMIN"))).andExpect(status().isInternalServerError()).andReturn();
        assertThat(result.getResponse().getContentAsString()).doesNotContain("private broker details","IllegalStateException");
    }
}
