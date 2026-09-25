package com.github.xingjianluo.downstreamcallresiliencejava.web;

import com.github.xingjianluo.downstreamcallresiliencejava.config.ResilienceProperties;
import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.model.OrchestrationResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestrator.OrchestrationRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestrator.Orchestrator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * 多下游编排对外接口。
 * <p>
 * 契约说明：即使部分下游降级，HTTP 状态码仍为 200，通过响应体中的 success / parts / degradedDownstreams
 * 表达部分结果与缺失原因，保证调用方总能在约定时限内得到结构稳定的响应。
 */
@RestController
@RequestMapping("/api/orchestrate")
public class OrchestrationController {

    private final Orchestrator orchestrator;
    private final ResilienceProperties properties;

    public OrchestrationController(Orchestrator orchestrator, ResilienceProperties properties) {
        this.orchestrator = orchestrator;
        this.properties = properties;
    }

    @PostMapping
    public ResponseEntity<OrchestrationResponse> orchestrate(@RequestBody OrchestrationRequestDto dto) {
        if (dto.getCalls() == null || dto.getCalls().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        List<DownstreamRequest> calls = dto.getCalls().stream()
                .map(c -> c.isWrite()
                        ? DownstreamRequest.write(c.getDownstream(), c.getPayload(), c.getIdempotencyKey())
                        : DownstreamRequest.read(c.getDownstream(), c.getPayload()))
                .toList();
        String requestId = dto.getRequestId() != null ? dto.getRequestId() : UUID.randomUUID().toString();
        long deadline = dto.getDeadlineMs() != null ? dto.getDeadlineMs() : properties.getOrchestrationDeadlineMs();

        OrchestrationResponse response = orchestrator.orchestrate(
                new OrchestrationRequest(requestId, calls, deadline));
        return ResponseEntity.ok(response);
    }
}
