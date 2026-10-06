package com.github.xingjianluo.downstreamcallresiliencejava.tests.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.DataSource;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.AggregatedResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartResult;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.status.DownstreamStatus;
import com.github.xingjianluo.downstreamcallresiliencejava.status.ResilienceStatusService;
import com.github.xingjianluo.downstreamcallresiliencejava.tests.support.TestFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 旧数据回退场景：
 * <ul>
 *   <li>读失败优先回退最近一次成功结果，标注 STALE 与年龄；</li>
 *   <li>从未成功过 -> 静态兜底；旧数据过期 -> 静态兜底；</li>
 *   <li>写操作（含副作用未知）绝不回退旧结果，与禁重试判定一致；</li>
 *   <li>失败结果不会被缓存；实时成功标注 LIVE；</li>
 *   <li>只读状态查询能看到熔断状态与回退缓存占用。</li>
 * </ul>
 */
class FallbackOrchestrationTest {

    private static final Logger log = LoggerFactory.getLogger(FallbackOrchestrationTest.class);

    private TestFixtures.Harness harness() {
        TestFixtures.Harness h = new TestFixtures.Harness();
        h.withPolicy(TestFixtures.policyWithFallback("read", Duration.ofMillis(200), 1, 10,
                Duration.ofHours(1), 1, OperationKind.READ, 8, Duration.ofMillis(100)));
        h.withPolicy(TestFixtures.policyWithFallback("writer", Duration.ofMillis(100), 5, 10,
                Duration.ofHours(1), 1, OperationKind.NON_IDEMPOTENT_WRITE, 8, Duration.ofHours(1)));
        h.withPolicy(TestFixtures.policyWithFallback("idem-writer", Duration.ofMillis(100), 5, 10,
                Duration.ofHours(1), 1, OperationKind.IDEMPOTENT_WRITE, 8, Duration.ofHours(1)));
        return h;
    }

    private PartSpec readPart() {
        return new PartSpec("read", new DownstreamRequest(null, "query-x"), () -> "static-default");
    }

    /** 让第 1 次调用成功，其余全部下游错误。 */
    private void succeedOnceThenFail(TestFixtures.Harness h, String name, String body) {
        h.server.script(name, (i, r) -> {
            if (i == 0) {
                return DownstreamResponse.ok(body);
            }
            throw MockDownstreamServer.downstreamError("500 boom");
        });
    }

    @Test
    void readFailureFallsBackToLastSuccessWithStaleMarkerAndAge() {
        TestFixtures.Harness h = harness();
        succeedOnceThenFail(h, "read", "fresh-v1");
        Orchestrator orch = h.orchestrator();

        AggregatedResponse first = orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));
        PartResult live = first.parts().get(0);
        assertTrue(live.present());
        assertEquals(DataSource.LIVE, live.source(), "首次成功必须标为实时数据");
        assertFalse(live.degraded());
        assertEquals(1, h.server.invocationCount("read"), "一次逻辑请求只打下游一次");

        h.clock.advanceMillis(40);
        AggregatedResponse second = orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));
        PartResult stale = second.parts().get(0);
        assertFalse(stale.present(), "旧数据回退仍是降级/缺实时结果，present=false 语义不变");
        assertTrue(stale.degraded());
        assertEquals(DataSource.STALE, stale.source(), "读失败必须优先回退旧数据");
        assertEquals("fresh-v1", stale.value());
        assertEquals(40, stale.staleAgeMillis(), "必须标出旧数据大概多久之前");
        assertEquals(FailureCategory.DOWNSTREAM_ERROR, stale.category());
        assertFalse(second.complete(), "complete/degraded/missing 语义保持不变");
        assertTrue(second.missing().get(0).contains("STALE"));
        log.info("旧数据回退: 来源=STALE age={}ms 值=fresh-v1 类别={}", stale.staleAgeMillis(), stale.category());
    }

    @Test
    void neverSucceededReadUsesStaticDefault() {
        TestFixtures.Harness h = harness();
        h.server.script("read", (i, r) -> {
            throw MockDownstreamServer.downstreamError("500 always");
        });
        Orchestrator orch = h.orchestrator();

        AggregatedResponse resp = orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));
        PartResult part = resp.parts().get(0);
        assertEquals(DataSource.STATIC_DEFAULT, part.source(), "从未成功过只能静态兜底");
        assertEquals("static-default", part.value());
        assertEquals(-1, part.staleAgeMillis());
        assertEquals(0, h.fallbacks.get("read").size(), "失败结果绝不允许被缓存");
        log.info("从未成功: 来源=STATIC_DEFAULT 降级值=static-default 缓存条数=0");
    }

    @Test
    void expiredStaleDataIsNotServedAndFallsBackToStatic() {
        TestFixtures.Harness h = harness();
        succeedOnceThenFail(h, "read", "fresh-v1");
        Orchestrator orch = h.orchestrator();

        orch.aggregate(List.of(readPart()), Duration.ofSeconds(2)); // 首次成功并缓存

        h.clock.advanceMillis(100);
        AggregatedResponse atBoundary = orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));
        assertEquals(DataSource.STALE, atBoundary.parts().get(0).source(),
                "age == ttl 边界仍可回退旧数据");

        h.clock.advanceMillis(1); // age=101 > ttl=100
        AggregatedResponse expired = orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));
        PartResult part = expired.parts().get(0);
        assertEquals(DataSource.STATIC_DEFAULT, part.source(), "过期旧数据不得再当有效数据回退");
        assertEquals("static-default", part.value());
        assertEquals(0, h.fallbacks.get("read").size(), "过期后占用必须释放");
        log.info("旧数据过期: age=101ms>ttl=100ms 来源=STATIC_DEFAULT 缓存已清理");
    }

    @Test
    void nonIdempotentWriteNeverReturnsStaleEvenWithPriorSuccess() {
        TestFixtures.Harness h = harness();
        // 即便该下游此前成功过，失败的写也绝不能拿旧结果充当成功
        h.server.script("writer", (i, r) -> {
            if (i == 0) {
                return DownstreamResponse.confirmedWrite("written-v1");
            }
            try {
                Thread.sleep(400); // 后续超时：副作用未知
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return DownstreamResponse.confirmedWrite("late");
        });
        Orchestrator orch = h.orchestrator();
        PartSpec write = new PartSpec("writer", new DownstreamRequest(null, "charge"),
                () -> "write-uncertain");

        AggregatedResponse first = orch.aggregate(List.of(write), Duration.ofSeconds(2));
        assertTrue(first.parts().get(0).present());
        assertEquals(0, h.fallbacks.get("writer").size(), "写结果绝不进入回退缓存");

        AggregatedResponse second = orch.aggregate(List.of(write), Duration.ofSeconds(2));
        PartResult part = second.parts().get(0);
        assertFalse(part.present());
        assertEquals(DataSource.STATIC_DEFAULT, part.source(),
                "副作用未知的写禁止回退旧结果，只能静态兜底并保持降级标记");
        assertEquals("write-uncertain", part.value());
        assertEquals(FailureCategory.TIMEOUT, part.category());
        assertEquals(1, part.attempts(), "非幂等写超时不得重试");
        assertTrue(h.rules.isSideEffectAmbiguous(OperationKind.NON_IDEMPOTENT_WRITE,
                FailureCategory.TIMEOUT, false), "降级与重试对副作用的判定必须一致");
        log.info("写降级一致: 来源=STATIC_DEFAULT 类别=TIMEOUT 尝试=1 判定=副作用未知(与禁重试一致)");
    }

    @Test
    void idempotentWriteAlsoDoesNotServeStaleOnFailure() {
        TestFixtures.Harness h = harness();
        h.server.script("idem-writer", (i, r) -> {
            if (i == 0) {
                return DownstreamResponse.confirmedWrite("ok-v1");
            }
            throw MockDownstreamServer.downstreamError("500 boom");
        });
        Orchestrator orch = h.orchestrator();
        PartSpec first = new PartSpec("idem-writer", new DownstreamRequest("idem-1", "create"),
                () -> "idem-default");
        // 第二次使用"新幂等键"：不会被下游幂等去重回放，真实走到失败脚本
        PartSpec second = new PartSpec("idem-writer", new DownstreamRequest("idem-2", "create"),
                () -> "idem-default");

        AggregatedResponse r1 = orch.aggregate(List.of(first), Duration.ofSeconds(2));
        assertTrue(r1.parts().get(0).present());
        AggregatedResponse r2 = orch.aggregate(List.of(second), Duration.ofSeconds(2));
        PartResult part = r2.parts().get(0);
        assertEquals(DataSource.STATIC_DEFAULT, part.source(), "幂等写失败也不回退旧结果");
        assertEquals(0, h.fallbacks.get("idem-writer").size());
        log.info("幂等写失败: 来源=STATIC_DEFAULT（写操作不享受旧数据回退）");
    }

    @Test
    void recoveredLiveCallRefreshesStaleData() {
        TestFixtures.Harness h = harness();
        // 成功 -> 失败(旧数据) -> 成功(实时)：最新成功值被刷新，之后再失败回退到新值
        h.server.script("read", (i, r) -> {
            if (i == 1) {
                throw MockDownstreamServer.downstreamError("500");
            }
            return DownstreamResponse.ok(i == 0 ? "v1" : "v2");
        });
        Orchestrator orch = h.orchestrator();

        orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));           // live v1
        AggregatedResponse stale1 = orch.aggregate(List.of(readPart()), Duration.ofSeconds(2)); // fail -> v1
        assertEquals("v1", stale1.parts().get(0).value());
        AggregatedResponse live2 = orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));  // live v2
        assertEquals(DataSource.LIVE, live2.parts().get(0).source());
        // 再制造一次失败：脚本中 i=3 成功，所以改为独立失败下游键验证刷新已发生
        assertEquals("v2", h.fallbacks.get("read").findValid("payload:query-x").value(),
                "实时成功必须刷新缓存中的旧数据");
        log.info("恢复后实时成功刷新旧数据: v1 -> v2");
    }

    @Test
    void overallDeadlineTimeoutOfSlowReadAlsoFallsBackToStale() {
        TestFixtures.Harness h = harness();
        // 首次快速成功；之后变得极慢，整体时限截断时应回退旧数据而非静态值
        h.server.script("read", (i, r) -> {
            if (i == 0) {
                return DownstreamResponse.ok("fast-snapshot");
            }
            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return DownstreamResponse.ok("late");
        });
        Orchestrator orch = h.orchestrator();
        PartSpec spec = readPart();

        orch.aggregate(List.of(spec), Duration.ofSeconds(2));
        h.clock.advanceMillis(10);
        long start = System.nanoTime();
        AggregatedResponse resp = orch.aggregate(List.of(spec), Duration.ofMillis(300));
        long elapsed = (System.nanoTime() - start) / 1_000_000;

        PartResult part = resp.parts().get(0);
        assertTrue(elapsed < 700, "整体时限仍须被遵守: " + elapsed + "ms");
        assertEquals(FailureCategory.TIMEOUT, part.category());
        assertEquals(DataSource.STALE, part.source(), "整体时限截断的慢读也要回退旧数据");
        assertEquals("fast-snapshot", part.value());
        assertEquals(10, part.staleAgeMillis());
        log.info("整体时限截断慢读: 类别=TIMEOUT 来源=STALE age=10ms 实际返回={}ms", elapsed);
    }

    @Test
    void statusEndpointShowsCircuitAndFallbackOccupancy() {
        TestFixtures.Harness h = harness();
        succeedOnceThenFail(h, "read", "fresh-v1");
        Orchestrator orch = h.orchestrator();
        orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));
        h.clock.advanceMillis(25);

        ResilienceStatusService statusService = new ResilienceStatusService(
                h.registry, h.fallbacks, h.clock);
        Map<String, DownstreamStatus> byName = statusService.snapshot().downstreams().stream()
                .collect(Collectors.toMap(DownstreamStatus::downstreamName, Function.identity()));

        DownstreamStatus read = byName.get("read");
        assertNotNull(read);
        assertEquals("READ", read.operationKind());
        assertEquals(1, read.fallbackSize(), "只读状态必须反映回退缓存占用");
        assertEquals(8, read.fallbackCapacity());
        assertEquals(100, read.fallbackTtlMillis());
        assertEquals(25, read.oldestEntryAgeMillis(), "必须能看到旧数据新鲜度");
        log.info("状态查询 read: 熔断={} 连续失败={} 回退占用={}/{} ttl={}ms 最旧年龄={}ms",
                read.circuitState(), read.consecutiveFailures(), read.fallbackSize(),
                read.fallbackCapacity(), read.fallbackTtlMillis(), read.oldestEntryAgeMillis());

        // 触发熔断后再查一次：状态变化可见
        h.server.script("read", (i, r) -> {
            throw MockDownstreamServer.downstreamError("500");
        });
        for (int i = 0; i < 10; i++) {
            orch.aggregate(List.of(readPart()), Duration.ofSeconds(2));
        }
        DownstreamStatus open = statusService.snapshot().downstreams().stream()
                .filter(s -> s.downstreamName().equals("read")).findFirst().orElseThrow();
        // 阈值 10（策略 failureThreshold=10），失败后应打开
        assertEquals(com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitState.OPEN,
                open.circuitState(), "只读状态必须反映熔断打开");
        assertTrue(open.circuitRecoverInMillis() > 0);
        log.info("状态查询 read 熔断后: 熔断=OPEN 恢复剩余={}ms 回退占用={}",
                open.circuitRecoverInMillis(), open.fallbackSize());
    }
}
