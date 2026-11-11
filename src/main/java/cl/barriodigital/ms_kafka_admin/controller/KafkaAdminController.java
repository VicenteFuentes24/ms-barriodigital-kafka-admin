package cl.barriodigital.ms_kafka_admin.controller;

import cl.barriodigital.ms_kafka_admin.dto.AdminDtos.*;
import cl.barriodigital.ms_kafka_admin.service.KafkaAdminService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka/admin")
public class KafkaAdminController {
    private final KafkaAdminService service;
    public KafkaAdminController(KafkaAdminService service) { this.service=service; }
    public record Csrf(String headerName, String parameterName, String token) { }
    @GetMapping("/csrf") public Csrf csrf(CsrfToken token) { return new Csrf(token.getHeaderName(),token.getParameterName(),token.getToken()); }
    @GetMapping("/cluster") public Cluster cluster() { return service.cluster(); }
    @GetMapping("/brokers") public List<Broker> brokers() { return service.brokers(); }
    @GetMapping("/topics") public List<TopicSummary> topics() { return service.topics(); }
    @GetMapping("/topics/{name}") public Topic topic(@PathVariable String name) { return service.topic(name); }
    @PostMapping("/topics") @ResponseStatus(HttpStatus.CREATED)
    public Action create(@Valid @RequestBody CreateTopic request) { return service.create(request); }
    @DeleteMapping("/topics/{name}") @ResponseStatus(HttpStatus.ACCEPTED)
    public Action delete(@PathVariable String name,@RequestHeader(value="X-Confirm-Topic",required=false) String confirmation) { return service.delete(name,confirmation); }
    @GetMapping("/topics/{name}/config") public List<ConfigValue> config(@PathVariable String name) { return service.config(name); }
    @PatchMapping("/topics/{name}/config") public Action config(@PathVariable String name,@Valid @RequestBody PatchConfig request) { return service.configure(name,request.values()); }
    @PostMapping("/topics/{name}/partitions") public Action partitions(@PathVariable String name,@Valid @RequestBody IncreasePartitions request) { return service.partitions(name,request.totalPartitions()); }
    @GetMapping("/consumer-groups") public List<GroupSummary> groups() { return service.groups(); }
    @GetMapping("/consumer-groups/{id}") public Group group(@PathVariable String id) { return service.group(id); }
    @GetMapping("/consumer-groups/{id}/offsets") public Offsets offsets(@PathVariable String id) { return service.offsets(id); }
    @GetMapping("/consumer-groups/{id}/lag") public Lag lag(@PathVariable String id) { return service.lag(id); }
}
