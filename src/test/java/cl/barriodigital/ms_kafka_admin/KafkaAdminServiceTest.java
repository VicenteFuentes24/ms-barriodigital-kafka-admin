package cl.barriodigital.ms_kafka_admin;

import cl.barriodigital.ms_kafka_admin.config.AdminProperties;
import cl.barriodigital.ms_kafka_admin.dto.AdminDtos.*;
import cl.barriodigital.ms_kafka_admin.exception.AdminApiException;
import cl.barriodigital.ms_kafka_admin.kafka.KafkaGateway;
import cl.barriodigital.ms_kafka_admin.service.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class KafkaAdminServiceTest {
    KafkaGateway kafka=mock(KafkaGateway.class);
    KafkaAdminService service=new KafkaAdminService(kafka,new TopicPolicy(new AdminProperties(true,Set.of(),500)));
    Node n1=new Node(1,"broker1",9092), n2=new Node(2,"broker2",9092), n3=new Node(3,"broker3",9092);
    TopicPartition p0=new TopicPartition("demo",0),p1=new TopicPartition("demo",1);
    TopicDescription description() { return new TopicDescription("demo",false,List.of(new TopicPartitionInfo(0,n1,List.of(n1,n2,n3),List.of(n1,n2)))); }
    ConsumerGroupDescription group(Set<TopicPartition> assigned) {
        var group=mock(ConsumerGroupDescription.class); when(group.groupState()).thenReturn(GroupState.EMPTY);
        when(group.coordinator()).thenReturn(n1);
        var member=mock(MemberDescription.class); when(member.consumerId()).thenReturn("member"); when(member.clientId()).thenReturn("client"); when(member.host()).thenReturn("localhost");
        when(member.assignment()).thenReturn(new MemberAssignment(assigned)); when(group.members()).thenReturn(assigned.isEmpty() ? List.of() : List.of(member));
        when(kafka.group("report")).thenReturn(group); return group;
    }
    ListOffsetsResult.ListOffsetsResultInfo info(long offset) { return new ListOffsetsResult.ListOffsetsResultInfo(offset,-1,Optional.empty()); }
    @Test void returnsRealClusterAndBrokerData() {
        when(kafka.cluster()).thenReturn(new KafkaGateway.ClusterData("id",n2,List.of(n1,n2,n3)));
        when(kafka.brokers()).thenReturn(List.of(n3,n1,n2));
        assertThat(service.cluster().controller().id()).isEqualTo(2);
        assertThat(service.brokers()).extracting(Broker::id).containsExactly(1,2,3);
    }
    @Test void listsAndInspectsPartitionsReplicasIsr() {
        when(kafka.topics()).thenReturn(List.of(new TopicListing("demo",Uuid.randomUuid(),false),new TopicListing("__internal",Uuid.randomUuid(),true)));
        when(kafka.topic("demo")).thenReturn(description());
        assertThat(service.topics()).filteredOn(TopicSummary::protectedTopic).hasSize(1);
        var partition=service.topic("demo").partitions().getFirst();
        assertThat(partition.replicas()).containsExactly(1,2,3); assertThat(partition.isr()).containsExactly(1,2);
        assertThat(partition.underReplicated()).isTrue();
    }
    @Test void createsWithValidatedReplicationAndConfiguration() {
        when(kafka.brokers()).thenReturn(List.of(n1,n2,n3));
        service.create(new CreateTopic("demo",3,3,86400000L,"delete",2));
        verify(kafka).create("demo",3,(short)3,Map.of("retention.ms","86400000","cleanup.policy","delete","min.insync.replicas","2"));
        assertThatThrownBy(() -> service.create(new CreateTopic("demo",3,4,1000L,"delete",2))).isInstanceOf(AdminApiException.class);
    }
    @Test void changesOnlyAllowedConfigurationAndIncreasesPartitions() {
        when(kafka.topic("demo")).thenReturn(description());
        service.configure("demo",Map.of("retention.ms","60000")); verify(kafka).alterConfig("demo",Map.of("retention.ms","60000"));
        service.partitions("demo",3); verify(kafka).partitions("demo",3);
        assertThatThrownBy(() -> service.partitions("demo",1)).isInstanceOf(AdminApiException.class);
        assertThatThrownBy(() -> service.configure("demo",Map.of("min.insync.replicas","4"))).isInstanceOf(AdminApiException.class);
    }
    @Test void deleteRequiresExactAdditionalConfirmation() {
        assertThatThrownBy(() -> service.delete("demo",null)).isInstanceOf(AdminApiException.class);
        assertThatThrownBy(() -> service.delete("demo","other")).isInstanceOf(AdminApiException.class);
        verifyNoInteractions(kafka);
        when(kafka.topic("demo")).thenReturn(description()); service.delete("demo","demo"); verify(kafka).delete("demo");
    }
    @Test void allWritesOnCriticalTopicStopBeforeCallingKafka() {
        String name="bd.requests.events.v1";
        assertThatThrownBy(() -> service.create(new CreateTopic(name,3,3,1000L,"delete",2))).isInstanceOf(AdminApiException.class);
        assertThatThrownBy(() -> service.delete(name,name)).isInstanceOf(AdminApiException.class);
        assertThatThrownBy(() -> service.partitions(name,6)).isInstanceOf(AdminApiException.class);
        assertThatThrownBy(() -> service.configure(name,Map.of("retention.ms","1"))).isInstanceOf(AdminApiException.class);
        verifyNoInteractions(kafka);
    }
    @Test void internallyMarkedTopicIsProtectedEvenWithoutPrefix() {
        when(kafka.topic("demo")).thenReturn(new TopicDescription("demo",true,List.of()));
        assertThatThrownBy(() -> service.delete("demo","demo")).isInstanceOf(AdminApiException.class); verify(kafka,never()).delete(anyString());
    }
    @Test void sensitiveConfigValuesAreRedacted() {
        var value=mock(ConfigEntry.class); when(value.name()).thenReturn("secret"); when(value.value()).thenReturn("do-not-expose");
        when(value.isSensitive()).thenReturn(true); when(value.source()).thenReturn(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
        var config=new Config(List.of(value));
        when(kafka.config("demo")).thenReturn(config);
        assertThat(service.config("demo").getFirst().value()).isNull();
    }
    @Test void listsGroupsAndMemberAssignments() {
        group(Set.of(p0)); when(kafka.groups()).thenReturn(List.of(new GroupListing("report",Optional.of(GroupType.CLASSIC),"consumer",Optional.of(GroupState.STABLE))));
        assertThat(service.groups().getFirst().groupId()).isEqualTo("report");
        assertThat(service.group("report").members().getFirst().assignments()).containsExactly(new Assignment("demo",0));
    }
    @Test void offsetsIncludeAssignedPartitionsWithoutCommits() {
        group(Set.of(p0,p1)); when(kafka.committed("report")).thenReturn(Map.of(p0,new OffsetAndMetadata(10)));
        assertThat(service.offsets("report").partitions()).extracting(Offset::committedOffset).containsExactly(10L,null);
    }
    @Test void inactiveGroupWithCommitsStillHasMeasurableLag() {
        group(Set.of()); when(kafka.committed("report")).thenReturn(Map.of(p0,new OffsetAndMetadata(10),p1,new OffsetAndMetadata(15)));
        when(kafka.offsets(anySet(),eq(false))).thenReturn(Map.of(p0,info(20),p1,info(15)));
        when(kafka.offsets(anySet(),eq(true))).thenReturn(Map.of(p0,info(0),p1,info(0)));
        assertThat(service.lag("report").totalLag()).isEqualTo(10); assertThat(service.lag("report").complete()).isTrue();
    }
    @Test void missingCommittedOffsetDoesNotInventZeroLag() {
        group(Set.of(p0)); when(kafka.committed("report")).thenReturn(Map.of());
        when(kafka.offsets(anySet(),eq(false))).thenReturn(Map.of(p0,info(20)));
        when(kafka.offsets(anySet(),eq(true))).thenReturn(Map.of(p0,info(0)));
        var lag=service.lag("report"); assertThat(lag.totalLag()).isNull(); assertThat(lag.complete()).isFalse();
        assertThat(lag.partitions().getFirst().status()).isEqualTo("NO_COMMITTED_OFFSET");
    }
    @Test void emptyGroupHasUnknownTotalNotInventedZero() {
        group(Set.of()); when(kafka.committed("report")).thenReturn(Map.of());
        assertThat(service.lag("report").totalLag()).isNull();
    }
    @Test void expiredAndAheadOffsetsHaveUnknownLag() {
        group(Set.of()); when(kafka.committed("report")).thenReturn(Map.of(p0,new OffsetAndMetadata(2),p1,new OffsetAndMetadata(200)));
        when(kafka.offsets(anySet(),eq(false))).thenReturn(Map.of(p0,info(20),p1,info(20)));
        when(kafka.offsets(anySet(),eq(true))).thenReturn(Map.of(p0,info(10),p1,info(0)));
        var lag=service.lag("report"); assertThat(lag.totalLag()).isNull();
        assertThat(lag.partitions()).extracting(PartitionLag::status).containsExactly("OFFSET_EXPIRED","OFFSET_AHEAD_OF_LOG");
    }
}
