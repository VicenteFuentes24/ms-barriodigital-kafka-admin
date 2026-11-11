package cl.barriodigital.ms_kafka_admin.kafka;

import cl.barriodigital.ms_kafka_admin.config.AdminProperties;
import cl.barriodigital.ms_kafka_admin.exception.AdminApiException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.*;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/** Only Kafka transport and bounded waiting; no domain or HTTP controller logic. */
@Component
public class KafkaGateway {
    private final Admin admin;
    private final AdminProperties properties;
    public KafkaGateway(Admin admin, AdminProperties properties) { this.admin=admin; this.properties=properties; }
    public record ClusterData(String id, Node controller, Collection<Node> nodes) { }
    public ClusterData cluster() {
        var result=admin.describeCluster();
        return new ClusterData(call("cluster_id",result::clusterId),call("controller",result::controller),call("brokers",result::nodes));
    }
    public Collection<Node> brokers() { return call("brokers",() -> admin.describeCluster().nodes()); }
    public Collection<TopicListing> topics() { return call("topics",() -> admin.listTopics(new ListTopicsOptions().listInternal(true)).listings()); }
    public TopicDescription topic(String name) {
        return call("topic",() -> admin.describeTopics(List.of(name)).allTopicNames()).get(name);
    }
    public Config config(String name) {
        var resource=new ConfigResource(ConfigResource.Type.TOPIC,name);
        return call("topic_config",() -> admin.describeConfigs(List.of(resource)).all()).get(resource);
    }
    public void create(String name, int partitions, short replicas, Map<String,String> config) {
        call("create_topic",() -> admin.createTopics(List.of(new NewTopic(name,partitions,replicas).configs(config))).all());
    }
    public void delete(String name) { call("delete_topic",() -> admin.deleteTopics(List.of(name)).all()); }
    public void partitions(String name, int count) {
        call("increase_partitions",() -> admin.createPartitions(Map.of(name,NewPartitions.increaseTo(count))).all());
    }
    public void alterConfig(String name, Map<String,String> values) {
        Collection<AlterConfigOp> changes=values.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(),e.getValue()),AlterConfigOp.OpType.SET)).toList();
        call("alter_config",() -> admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC,name),changes)).all());
    }
    public Collection<GroupListing> groups() {
        return call("consumer_groups",() -> admin.listGroups().all()).stream()
                .filter(g -> g.isSimpleConsumerGroup() || "consumer".equals(g.protocol()) || g.type().orElse(null)==GroupType.CONSUMER).toList();
    }
    public ConsumerGroupDescription group(String id) {
        var group=call("consumer_group",() -> admin.describeConsumerGroups(List.of(id)).all()).get(id);
        if (group==null || group.groupState()==GroupState.DEAD)
            throw new AdminApiException(HttpStatus.NOT_FOUND,"NOT_FOUND","Consumer group not found");
        return group;
    }
    public Map<TopicPartition,OffsetAndMetadata> committed(String id) {
        return call("committed_offsets",() -> admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata());
    }
    public Map<TopicPartition,ListOffsetsResult.ListOffsetsResultInfo> offsets(Set<TopicPartition> partitions, boolean earliest) {
        if (partitions.isEmpty()) return Map.of();
        Map<TopicPartition,OffsetSpec> specs=new HashMap<>();
        partitions.forEach(p -> specs.put(p,earliest ? OffsetSpec.earliest() : OffsetSpec.latest()));
        return call("log_offsets",() -> admin.listOffsets(specs).all());
    }
    private <T> T call(String operation, Supplier<KafkaFuture<T>> request) {
        try { return request.get().get(properties.timeoutMs(),TimeUnit.MILLISECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw unavailable(); }
        catch (Exception e) {
            Throwable cause=e instanceof ExecutionException && e.getCause()!=null ? e.getCause() : e;
            LoggerFactory.getLogger(getClass()).warn("kafka_admin_failed operation={} category={}", operation,cause.getClass().getSimpleName());
            if (cause instanceof UnknownTopicOrPartitionException || cause instanceof GroupIdNotFoundException)
                throw new AdminApiException(HttpStatus.NOT_FOUND,"NOT_FOUND","Kafka resource not found");
            if (cause instanceof TopicExistsException || cause instanceof InvalidPartitionsException)
                throw new AdminApiException(HttpStatus.CONFLICT,"CONFLICT","Resource already exists or partition state changed; inspect before retrying");
            if (cause instanceof AuthorizationException)
                throw new AdminApiException(HttpStatus.FORBIDDEN,"KAFKA_AUTHORIZATION_DENIED","Kafka denied this administrative operation");
            if (cause instanceof InvalidTopicException || cause instanceof InvalidConfigurationException
                    || cause instanceof InvalidReplicationFactorException || cause instanceof IllegalArgumentException)
                throw AdminApiException.invalid("Kafka rejected the requested topic configuration");
            throw unavailable();
        }
    }
    private AdminApiException unavailable() {
        return new AdminApiException(HttpStatus.SERVICE_UNAVAILABLE,"KAFKA_UNAVAILABLE",
                "Kafka unavailable or operation timed out. A write may have completed; inspect its state before retrying");
    }
}
