package com.github.xingjianluo.downstreamcallresiliencejava.web;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheStats;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.AggregatedResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitBreakerRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 演示用对外接口。
 * <p>既有接口契约保持不变，仅新增路径：
 * <ul>
 *   <li>GET  /api/resilience/aggregate：并发编排三个下游，返回（可能带降级的）聚合结果；</li>
 *   <li>GET  /api/resilience/status：只读查询各下游熔断状态与回退缓存占用；</li>
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
    private final CircuitBreakerRegistry breakers;
    private final FallbackCacheRegistry fallbackCaches;

    public DemoController(Orchestrator orchestrator, MockDownstreamServer server,
                          CircuitBreakerRegistry breakers, FallbackCacheRegistry fallbackCaches) {
        this.orchestrator = orchestrator;
        this.server = server;
        this.breakers = breakers;
        this.fallbackCaches = fallbackCaches;
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

    /**
     * 只读查询入口：各下游当前熔断状态与回退缓存占用，便于本地复现与排查。
     */
    @GetMapping("/status")
    public Map<String, DownstreamStatus> status() {
        Map<String, String> circuits = breakers.snapshots();
        Map<String, FallbackCacheStats> caches = fallbackCaches.stats();
        Map<String, DownstreamStatus> result = new LinkedHashMap<>();
        circuits.forEach((name, circuit) -> {
            FallbackCacheStats cache = caches.get(name);
            result.put(name, new DownstreamStatus(circuit,
                    cache == null ? 0 : cache.size(),
                    cache == null ? 0 : cache.capacity(),
                    cache == null ? 0 : cache.ttlMillis()));
        });
        caches.forEach((name, cache) ->
                result.putIfAbsent(name, new DownstreamStatus("(尚未发起调用)",
                        cache.size(), cache.capacity(), cache.ttlMillis())));
        return result;
    }

    /**
     * 单个下游的只读状态视图：熔断快照 + 回退缓存占用。
     */
    public record DownstreamStatus(String circuit, int fallbackSize,
                                   int fallbackCapacity, long fallbackTtlMillis) {
    }

    @PostMapping("/demo/reset")
    public String reset() {
        installScripts();
        return "ok";
    }
}
