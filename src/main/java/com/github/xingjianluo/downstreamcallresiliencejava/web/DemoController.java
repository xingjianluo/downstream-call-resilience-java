package com.github.xingjianluo.downstreamcallresiliencejava.web;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.AggregatedResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartSpec;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;

/**
 * 演示用对外接口。
 * <p>既有接口契约保持不变，仅新增路径：
 * <ul>
 *   <li>GET  /api/resilience/aggregate：并发编排三个下游，返回（可能带降级的）聚合结果；</li>
 *   <li>POST /api/resilience/demo/reset：重置模拟下游脚本，便于本地反复复现。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/resilience")
public class DemoController {

    static final String HEALTHY = "profile-service";
    static final String FLAKY = "inventory-service";
    static final String WRITER = "order-write-service";

    private final Orchestrator orchestrator;
    private final MockDownstreamServer server;

    public DemoController(Orchestrator orchestrator, MockDownstreamServer server) {
        this.orchestrator = orchestrator;
        this.server = server;
        installScripts();
    }

    private void installScripts() {
        // 健康下游：始终快速成功
        server.script(HEALTHY, (index, request) -> DownstreamResponse.ok("profile-data"));
        // 持续故障下游：稳定返回下游错误，用于展示重试耗尽后降级与熔断
        server.script(FLAKY, (index, request) -> {
            throw MockDownstreamServer.downstreamError("inventory 500: 库存查询失败");
        });
        // 写下游：首次确认生效；相同幂等键再次调用将由服务器回放重复响应（写只执行一次）
        server.script(WRITER, (index, request) -> DownstreamResponse.confirmedWrite("order-created"));
    }

    @GetMapping("/aggregate")
    public AggregatedResponse aggregate() {
        List<PartSpec> parts = List.of(
                new PartSpec(HEALTHY, new DownstreamRequest(null, "query-profile"),
                        () -> "profile-cached"),
                new PartSpec(FLAKY, new DownstreamRequest(null, "query-inventory"),
                        () -> "inventory-unknown"),
                new PartSpec(WRITER, new DownstreamRequest("idem-demo-001", "create-order"),
                        () -> "order-uncertain"));
        return orchestrator.aggregate(parts, Duration.ofMillis(1500));
    }

    @PostMapping("/demo/reset")
    public String reset() {
        installScripts();
        return "ok";
    }
}
