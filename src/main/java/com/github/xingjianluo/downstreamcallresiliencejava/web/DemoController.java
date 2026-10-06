package com.github.xingjianluo.downstreamcallresiliencejava.web;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.AggregatedResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.status.ResilienceStatus;
import com.github.xingjianluo.downstreamcallresiliencejava.status.ResilienceStatusService;
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
 *   <li>GET  /api/resilience/status：只读查看各下游健康/熔断状态与回退缓存占用；</li>
 *   <li>POST /api/resilience/demo/reset：重置模拟脚本与回退缓存，便于本地反复复现。</li>
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
    private final ResilienceStatusService statusService;
    private final FallbackCacheRegistry fallbackRegistry;

    public DemoController(Orchestrator orchestrator, MockDownstreamServer server,
                          ResilienceStatusService statusService,
                          FallbackCacheRegistry fallbackRegistry) {
        this.orchestrator = orchestrator;
        this.server = server;
        this.statusService = statusService;
        this.fallbackRegistry = fallbackRegistry;
        installScripts();
    }

    private void installScripts() {
        // 健康下游：始终快速成功
        server.script(HEALTHY, (index, request) -> DownstreamResponse.ok("profile-data"));
        // 故障读下游：第 1 次成功（其结果会作为旧数据缓存），之后持续下游错误。
        // 于是第 1 次聚合是实时数据；之后请求先回退旧数据(STALE)，旧数据过期后才静态兜底。
        server.script(FLAKY, (index, request) -> {
            if (index == 0) {
                return DownstreamResponse.ok("inventory-snapshot-v1");
            }
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

    /**
     * 只读：各下游熔断状态（CLOSED/OPEN/HALF_OPEN、连续失败、恢复剩余）与
     * 回退缓存占用（有效条数/容量/有效期/最旧条目年龄）。
     */
    @GetMapping("/status")
    public ResilienceStatus status() {
        return statusService.snapshot();
    }

    @PostMapping("/demo/reset")
    public String reset() {
        installScripts();
        fallbackRegistry.clear(HEALTHY);
        fallbackRegistry.clear(FLAKY);
        fallbackRegistry.clear(WRITER);
        return "ok";
    }
}
