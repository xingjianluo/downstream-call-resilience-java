package com.github.xingjianluo.downstreamcallresiliencejava.tests;

import com.github.xingjianluo.downstreamcallresiliencejava.breaker.CircuitState;
import com.github.xingjianluo.downstreamcallresiliencejava.caller.DownstreamCaller;
import com.github.xingjianluo.downstreamcallresiliencejava.idempotency.SideEffectLedger;
import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind;
import com.github.xingjianluo.downstreamcallresiliencejava.model.OrchestrationResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.model.PartResult;
import com.github.xingjianluo.downstreamcallresiliencejava.model.PartStatus;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestrator.OrchestrationRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestrator.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.policy.DownstreamPolicy;
import com.github.xingjianluo.downstreamcallresiliencejava.policy.PolicyRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.DownstreamSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.ScriptStep;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.SimulatedDownstreamTransport;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 请求级降级：
 * 健康下游 OK、故障下游 DEGRADED，整体仍在 deadline 内返回；
 * 降级部分必须带 reason / reasonMessage，且写副作用状态与重试路径一致（来自同一个台账）。
 */
class OrchestrationDegradeTest {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationDegradeTest.class);

    private TestContext buildContext() {
        SimulatedDownstreamTransport transport = new SimulatedDownstreamTransport(Map.of(
                "inventory", new DownstreamSpec("inventory", true, List.of(ScriptStep.ok("item-1"))),
                "pricing", new DownstreamSpec("pricing", true,
                        List.of(ScriptStep.latency("late", 600))),
                "promotion", new DownstreamSpec("promotion", true,
                        List.of(ScriptStep.error(2, "PROMO_500")))));
        PolicyRegistry registry = new PolicyRegistry();
        registry.register(policy("inventory", 300, 2, 10));
        registry.register(policy("pricing", 100, 1, 10));
        registry.register(policy("promotion", 300, 1, 10));
        SideEffectLedger ledger = new SideEffectLedger();
        DownstreamCaller caller = new DownstreamCaller(transport, registry, ledger,
                new ScheduledThreadPoolExecutor(2));
        Orchestrator orchestrator = new Orchestrator(caller,
                Executors.newVirtualThreadPerTaskExecutor());
        return new TestContext(transport, caller, orchestrator);
    }

    private DownstreamPolicy policy(String name, long timeoutMs, int attempts, int threshold) {
        DownstreamPolicy p = new DownstreamPolicy();
        p.setName(name);
        p.setTimeoutMs(timeoutMs);
        p.setReadMaxAttempts(attempts);
        p.setWriteMaxAttempts(attempts);
        p.setFailureThreshold(threshold);
        p.setOpenStateMs(10_000);
        p.setRetryBackoffMs(5);
        return p;
    }

    private record TestContext(SimulatedDownstreamTransport transport,
                               DownstreamCaller caller, Orchestrator orchestrator) {
    }

    @Test
    void returnsPartialResultWithinDeadlineWithReasons() {
        TestContext ctx = buildContext();
        OrchestrationRequest req = new OrchestrationRequest("req-1", List.of(
                DownstreamRequest.read("inventory", "sku"),
                DownstreamRequest.read("pricing", "sku"),
                DownstreamRequest.read("promotion", "sku")), 400);

        OrchestrationResponse resp = ctx.orchestrator().orchestrate(req);

        for (PartResult part : resp.parts()) {
            log.info("[编排部分] downstream={} status={} value={} reason={} message={} attempts={} basis=并行汇聚后的每下游判定",
                    part.downstream(), part.status(), part.value(), part.reason(),
                    part.reasonMessage(), part.attempts());
        }
        log.info("[编排整体] success={} elapsedMs={}/{} degraded={} basis=部分失败也在约定时限内返回完整结构",
                resp.success(), resp.elapsedMs(), resp.deadlineMs(), resp.degradedDownstreams());

        assertFalse(resp.success());
        assertTrue(resp.elapsedMs() <= resp.deadlineMs() + 100,
                "必须在约定时限附近返回，实际 " + resp.elapsedMs());
        assertEquals(List.of("pricing", "promotion"), resp.degradedDownstreams());

        PartResult inventory = part(resp, "inventory");
        assertEquals(PartStatus.OK, inventory.status());
        assertEquals("item-1", inventory.value());

        PartResult pricing = part(resp, "pricing");
        assertEquals(PartStatus.DEGRADED, pricing.status());
        assertEquals(FailureKind.TIMEOUT, pricing.reason(), "失败原因须可区分");
        assertFalse(pricing.reasonMessage().isBlank());

        PartResult promotion = part(resp, "promotion");
        assertEquals(PartStatus.DEGRADED, promotion.status());
        assertEquals(FailureKind.DOWNSTREAM_ERROR, promotion.reason());
    }

    @Test
    void degradedWritePartCarriesConsistentSideEffectFlag() {
        SimulatedDownstreamTransport transport = new SimulatedDownstreamTransport(Map.of(
                "orders", new DownstreamSpec("orders", true,
                        List.of(ScriptStep.responseLost(5)))));
        PolicyRegistry registry = new PolicyRegistry();
        registry.register(policy("orders", 500, 1, 10));
        DownstreamCaller caller = new DownstreamCaller(transport, registry, new SideEffectLedger(),
                new ScheduledThreadPoolExecutor(1));
        Orchestrator orchestrator = new Orchestrator(caller,
                Executors.newVirtualThreadPerTaskExecutor());

        OrchestrationResponse resp = orchestrator.orchestrate(new OrchestrationRequest("req-2",
                List.of(DownstreamRequest.write("orders", "order:77", "key-77")), 800));

        PartResult part = part(resp, "orders");
        log.info("[编排部分] downstream=orders status={} reason={} sideEffectConfirmed={} serverExecutions={} basis=RESPONSE_LOST 下降级原因与副作用判定来自同一台账，重试与降级口径一致",
                part.status(), part.reason(), part.sideEffectConfirmed(),
                transport.serverExecutions("orders"));
        assertEquals(PartStatus.DEGRADED, part.status());
        assertEquals(FailureKind.RESPONSE_LOST, part.reason());
        assertEquals(1, transport.serverExecutions("orders"), "响应丢失也只执行一次");
        assertFalse(part.sideEffectConfirmed(), "响应丢失时不得谎称副作用已确认");
    }

    @Test
    void degradedDueToOpenCircuitIsFastAndLabeled() {
        TestContext ctx = buildContext();
        // 先把 promotion 熔断打开
        for (int i = 0; i < 10; i++) {
            ctx.caller().call(DownstreamRequest.read("promotion", "sku"));
        }
        assertEquals(CircuitState.OPEN, ctx.caller().circuitBreaker("promotion").currentState());

        OrchestrationResponse resp = ctx.orchestrator().orchestrate(new OrchestrationRequest("req-3",
                List.of(
                        DownstreamRequest.read("inventory", "sku"),
                        DownstreamRequest.read("promotion", "sku")), 400));

        PartResult promotion = part(resp, "promotion");
        log.info("[编排部分] downstream=promotion status={} reason={} basis=熔断打开时该部分快速失败并标记 CIRCUIT_OPEN，不拖累整体时限",
                promotion.status(), promotion.reason());
        assertEquals(FailureKind.CIRCUIT_OPEN, promotion.reason());
        assertEquals(PartStatus.DEGRADED, promotion.status());
        assertTrue(resp.elapsedMs() < 200);
    }

    private static PartResult part(OrchestrationResponse resp, String name) {
        return resp.parts().stream()
                .filter(p -> p.downstream().equals(name))
                .findFirst()
                .orElseThrow();
    }
}
