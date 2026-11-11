package cl.barriodigital.ms_kafka_admin;

import cl.barriodigital.ms_kafka_admin.config.AdminProperties;
import cl.barriodigital.ms_kafka_admin.exception.AdminApiException;
import cl.barriodigital.ms_kafka_admin.kafka.KafkaGateway;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class KafkaGatewayTest {
    Admin admin=mock(Admin.class,RETURNS_DEEP_STUBS);
    KafkaGateway gateway=new KafkaGateway(admin,new AdminProperties(false,Set.of(),500));
    <T> KafkaFuture<T> failed(Throwable error) { var future=new KafkaFutureImpl<T>(); future.completeExceptionally(error); return future; }
    @Test void brokerReadUsesDescribeCluster() {
        when(admin.describeCluster().nodes()).thenReturn(KafkaFuture.completedFuture(List.of(new Node(1,"localhost",19092))));
        assertThat(gateway.brokers()).hasSize(1);
    }
    @Test void mapsKafkaFailuresWithoutLeakingMessages() {
        for (var entry : Map.of(new UnknownTopicOrPartitionException("private detail"),404,
                new TopicExistsException("private detail"),409,new ClusterAuthorizationException("private detail"),403,
                new InvalidConfigurationException("private detail"),400,new org.apache.kafka.common.errors.TimeoutException("private detail"),503).entrySet()) {
            when(admin.describeTopics(List.of("demo")).allTopicNames()).thenReturn(failed(entry.getKey()));
            assertThatThrownBy(() -> gateway.topic("demo")).isInstanceOfSatisfying(AdminApiException.class,
                    e -> { assertThat(e.status().value()).isEqualTo(entry.getValue()); assertThat(e.getMessage()).doesNotContain("private detail"); });
        }
    }
    @Test void boundedWaitingOnUncompletedFuture() {
        when(admin.describeCluster().nodes()).thenReturn(new KafkaFutureImpl<>());
        long start=System.nanoTime();
        assertThatThrownBy(gateway::brokers).isInstanceOfSatisfying(AdminApiException.class,e -> assertThat(e.status().value()).isEqualTo(503));
        assertThat((System.nanoTime()-start)/1_000_000).isBetween(400L,3000L);
    }
    @Test void usesIncrementalConfigSetNotFullReplacement() {
        when(admin.incrementalAlterConfigs(anyMap()).all()).thenReturn(KafkaFuture.completedFuture(null));
        gateway.alterConfig("demo",Map.of("retention.ms","5000"));
        verify(admin).incrementalAlterConfigs(argThat(changes -> changes.size()==1 && changes.values().iterator().next().stream()
                .allMatch(op -> op.opType()==AlterConfigOp.OpType.SET && op.configEntry().name().equals("retention.ms") && op.configEntry().value().equals("5000"))));
    }
    @Test void createDeleteAndPartitionsUseAdminOperations() {
        when(admin.createTopics(anyCollection()).all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteTopics(anyCollection()).all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createPartitions(anyMap()).all()).thenReturn(KafkaFuture.completedFuture(null));
        clearInvocations(admin);
        gateway.create("demo",3,(short)3,Map.of("retention.ms","1000"));
        verify(admin).createTopics(argThat(topics -> topics!=null && !topics.isEmpty() && topics.iterator().next().replicationFactor()==3 && topics.iterator().next().numPartitions()==3));
        gateway.delete("demo"); verify(admin).deleteTopics(List.of("demo"));
        gateway.partitions("demo",4); verify(admin).createPartitions(argThat(p -> p!=null && p.get("demo")!=null && p.get("demo").totalCount()==4));
    }
    @Test void absentConsumerGroupIsNotFound() {
        var description=mock(ConsumerGroupDescription.class); when(description.groupState()).thenReturn(GroupState.DEAD);
        when(admin.describeConsumerGroups(List.of("missing")).all()).thenReturn(KafkaFuture.completedFuture(Map.of("missing",description)));
        assertThatThrownBy(() -> gateway.group("missing")).isInstanceOfSatisfying(AdminApiException.class,e -> assertThat(e.status().value()).isEqualTo(404));
    }
    @Test void listGroupsFiltersNonConsumerProtocols() {
        when(admin.listGroups().all()).thenReturn(KafkaFuture.completedFuture(List.of(
                new GroupListing("report",Optional.of(GroupType.CLASSIC),"consumer",Optional.of(GroupState.STABLE)),
                new GroupListing("connect",Optional.of(GroupType.CLASSIC),"connect",Optional.of(GroupState.STABLE)))));
        assertThat(gateway.groups()).extracting(GroupListing::groupId).containsExactly("report");
    }
}
