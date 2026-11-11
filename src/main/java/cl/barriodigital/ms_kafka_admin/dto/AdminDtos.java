package cl.barriodigital.ms_kafka_admin.dto;

import jakarta.validation.constraints.*;
import java.time.Instant;
import java.util.*;

public final class AdminDtos {
    private AdminDtos() { }
    public record Broker(int id, String host, int port, String rack) { }
    public record Cluster(String clusterId, Broker controller, List<Broker> brokers, Instant observedAt) { }
    public record TopicSummary(String name, boolean internal, boolean protectedTopic) { }
    public record Partition(int partition, Integer leader, List<Integer> replicas, List<Integer> isr, boolean underReplicated) { }
    public record Topic(String name, boolean internal, boolean protectedTopic, List<Partition> partitions) { }
    public record ConfigValue(String name, String value, boolean sensitive, boolean readOnly, String source) { }
    public record CreateTopic(@NotBlank String name, @NotNull @Min(1) @Max(1000) Integer partitions,
            @NotNull @Min(1) @Max(32767) Integer replicationFactor,
            @NotNull @Min(1) Long retentionMs, @NotBlank String cleanupPolicy,
            @NotNull @Min(1) Integer minInsyncReplicas) { }
    public record PatchConfig(@NotEmpty Map<@NotBlank String,@NotBlank String> values) { }
    public record IncreasePartitions(@NotNull @Min(1) @Max(1000) Integer totalPartitions) { }
    public record Action(String topic, String operation, String status) { }
    public record Assignment(String topic, int partition) { }
    public record Member(String memberId, String clientId, String host, List<Assignment> assignments) { }
    public record GroupSummary(String groupId, String state) { }
    public record Group(String groupId, String state, Broker coordinator, List<Member> members) { }
    public record Offset(String topic, int partition, Long committedOffset) { }
    public record Offsets(String groupId, List<Offset> partitions, Instant observedAt) { }
    public record PartitionLag(String topic, int partition, Long committedOffset, Long earliestOffset,
            Long latestOffset, Long lag, String status) { }
    public record Lag(String groupId, List<PartitionLag> partitions, Long totalLag, boolean complete, Instant observedAt) { }
}
