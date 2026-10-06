package com.github.xingjianluo.downstreamcallresiliencejava.tests.fallback;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackPolicy;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.AggregatedResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartResult;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartSource;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.tests.support.TestFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 编排层旧数据回退场景：
 * <ul>
 *   <li>读操作失败优先回退最近一次成功结果，标注来源与新鲜度；</li>
 *   <li>从未成功过 / 旧数据过期 / 写操作：仍用静态缺省值；</li>
 *   <li>写操作绝不回退旧数据，与禁重试判定一致；</li>
 *   <li>并发下不把下游打两次、失败结果不进缓存、容量与计数一致；</li>
 *   <li>整体时限截断的分片同样可走旧数据回退。</li>
 * </ul>
 */
class StaleFallbackOrchestrationTest {

    private static final Logger log = LoggerFactory.getLogger(StaleFallbackOrchestrationTest.class);

    private static final Duration TTL = Duration.ofSeconds(30);

    private TestFixtures.Harness harness() {
        return new TestFixtures.Harness();
    }

    private FallbackCacheRegistry caches(TestFixtures.Harness h, int capacity) {
        return new FallbackCacheRegistry(name -> new FallbackPolicy(capacity, TTL), h.clock);
    }

    private void readPolicy(TestFixtures.Harness h, String name) {
        h.withPolicy(TestFixtures.policy(name, Duration.ofMillis(100), 1, 10,
                Duration.ofHours(1), 1, OperationKind.READ));
    }

    private PartResult onlyPart(AggregatedResponse r) {
        assertEquals(1, r.parts().size());
        return r.parts().get(0);
    }

    @Test
    void readFailureFallsBackToLastSuccessWithFreshness() {
        TestFixtures.Harness h = harness();
        readPolicy(h, "reader");
        FallbackCacheRegistry caches = caches(h, 8);
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport(), caches, h.rules);

        // 先成功一次：结果被记入回退缓存
        h.server.script("reader", (i, r) -> DownstreamResponse.ok("real-data-v1"));
        PartSpec spec = new PartSpec("reader", new DownstreamRequest(null, "q1"),
                () -> "static-default");
        PartResult first = onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1)));
        assertTrue(first.present());
        assertEquals(PartSource.LIVE, first.source(), "成功分片来源必须是实时");
        assertNull(first.ageMillis());

        // 数据"变老" 5 秒后下游故障：回退到最近一次成功结果
        h.clock.advanceMillis(5_000);
        h.server.script("reader", (i, r) -> {
            throw MockDownstreamServer.downstreamError("500 boom");
        });
        PartResult degraded = onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1)));

        assertFalse(degraded.present());
        assertTrue(degraded.degraded());
        assertEquals(PartSource.STALE_FALLBACK, degraded.source(), "必须标明是旧数据回退");
        assertEquals("real-data-v1", degraded.value(), "回退值必须是最近一次成功结果而非写死占位");
        assertEquals(5_000L, degraded.ageMillis(), "必须标明数据大概是多久之前的");
        assertEquals(FailureCategory.DOWNSTREAM_ERROR, degraded.category());
        assertTrue(degraded.reason().contains("5000ms"), "判定依据需包含新鲜度: " + degraded.reason());
        log.info("[reader] 本次=旧数据回退 值={} 数据年龄={}ms 依据={}",
                degraded.value(), degraded.ageMillis(), degraded.reason());
    }

    @Test
    void neverSucceededUsesStaticDefault() {
        TestFixtures.Harness h = harness();
        readPolicy(h, "reader");
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport(), caches(h, 8), h.rules);

        h.server.script("reader", (i, r) -> {
            throw MockDownstreamServer.downstreamError("500 boom");
        });
        PartResult part = onlyPart(orchestrator.aggregate(
                List.of(new PartSpec("reader", new DownstreamRequest(null, "q1"),
                        () -> "static-default")), Duration.ofSeconds(1)));

        assertEquals(PartSource.STATIC_FALLBACK, part.source(), "从未成功过只能用静态兜底");
        assertEquals("static-default", part.value());
        assertNull(part.ageMillis());
        log.info("[reader] 本次=静态兜底 依据=从未成功过，无旧数据可回退");
    }

    @Test
    void expiredCacheUsesStaticDefault() {
        TestFixtures.Harness h = harness();
        readPolicy(h, "reader");
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport(), caches(h, 8), h.rules);

        h.server.script("reader", (i, r) -> DownstreamResponse.ok("real-data-v1"));
        PartSpec spec = new PartSpec("reader", new DownstreamRequest(null, "q1"),
                () -> "static-default");
        assertTrue(onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1))).present());

        // 旧数据过期后下游故障：不得再回退，用静态兜底
        h.clock.advanceMillis(TTL.toMillis() + 1);
        h.server.script("reader", (i, r) -> {
            throw MockDownstreamServer.downstreamError("500 boom");
        });
        PartResult part = onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1)));

        assertEquals(PartSource.STATIC_FALLBACK, part.source(), "过期旧数据不得再当有效数据回退");
        assertEquals("static-default", part.value());
        assertNull(part.ageMillis());
        log.info("[reader] 本次=静态兜底 依据=旧数据已过期(TTL={}ms)", TTL.toMillis());
    }

    @Test
    void writeOperationNeverFallsBackToStaleResult() {
        TestFixtures.Harness h = harness();
        h.withPolicy(TestFixtures.policy("writer", Duration.ofMillis(100), 1, 10,
                Duration.ofHours(1), 1, OperationKind.NON_IDEMPOTENT_WRITE));
        FallbackCacheRegistry caches = caches(h, 8);
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport(), caches, h.rules);

        PartSpec spec = new PartSpec("writer", new DownstreamRequest(null, "charge"),
                () -> "write-uncertain");

        // 写成功也不进回退缓存
        h.server.script("writer", (i, r) -> DownstreamResponse.confirmedWrite("written"));
        assertTrue(onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1))).present());
        assertEquals(0, caches.stats().getOrDefault("writer",
                new com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheStats(0, 8, 0)).size(),
                "写操作结果不得进入回退缓存");

        // 写失败后即使"曾经有过成功结果"也绝不回退旧数据
        h.server.script("writer", (i, r) -> {
            throw MockDownstreamServer.downstreamError("charge failed");
        });
        PartResult part = onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1)));

        assertEquals(PartSource.STATIC_FALLBACK, part.source(), "写操作绝不回退旧数据");
        assertEquals("write-uncertain", part.value());
        // 与重试/副作用判定同源：非幂等写不允许旧数据回退，正如其结果未知时禁止重试
        assertFalse(h.rules.isStaleFallbackAllowed(OperationKind.NON_IDEMPOTENT_WRITE));
        assertFalse(h.rules.isStaleFallbackAllowed(OperationKind.IDEMPOTENT_WRITE));
        assertTrue(h.rules.isStaleFallbackAllowed(OperationKind.READ));
        assertTrue(h.rules.isSideEffectAmbiguous(OperationKind.NON_IDEMPOTENT_WRITE,
                FailureCategory.DOWNSTREAM_ERROR, false));
        log.info("[writer] 本次=静态兜底 判定=写操作不适合回退旧数据（与禁重试同一份规则）");
    }

    @Test
    void overallDeadlineCutoffAlsoUsesStaleFallback() {
        TestFixtures.Harness h = harness();
        h.withPolicy(TestFixtures.policy("slow", Duration.ofMillis(500), 1, 10,
                Duration.ofHours(1), 1, OperationKind.READ));
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport(), caches(h, 8), h.rules);

        PartSpec spec = new PartSpec("slow", new DownstreamRequest(null, "q1"),
                () -> "static-default");
        h.server.script("slow", (i, r) -> DownstreamResponse.ok("cached-slow-data"));
        assertTrue(onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1))).present());

        // 之后变慢：整体时限截断，但仍可回退旧数据
        h.clock.advanceMillis(2_000);
        h.server.script("slow", (i, r) -> {
            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return DownstreamResponse.ok("late");
        });
        long start = System.nanoTime();
        PartResult part = onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofMillis(300)));
        long elapsed = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsed < 900, "整体时限不变，不得被回退逻辑拖延: " + elapsed + "ms");
        assertEquals(FailureCategory.TIMEOUT, part.category());
        assertEquals(PartSource.STALE_FALLBACK, part.source(), "整体时限截断的读分片同样回退旧数据");
        assertEquals("cached-slow-data", part.value());
        assertEquals(2_000L, part.ageMillis());
        log.info("[slow] 整体时限截断 本次=旧数据回退 值={} 数据年龄={}ms 耗时={}ms",
                part.value(), part.ageMillis(), elapsed);
    }

    @Test
    void concurrentFailuresNeverDuplicateDownstreamCallsNorCacheFailures() throws Exception {
        TestFixtures.Harness h = harness();
        // 熔断阈值调大，保证每个请求都真实打到下游（不被熔断快速失败干扰计数）
        h.withPolicy(TestFixtures.policy("reader", Duration.ofMillis(100), 1, 10_000,
                Duration.ofHours(1), 1, OperationKind.READ));
        int capacity = 4;
        FallbackCacheRegistry caches = caches(h, capacity);
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport(), caches, h.rules);

        // 先成功一次，缓存里只有这 1 条成功结果
        h.server.script("reader", (i, r) -> DownstreamResponse.ok("good"));
        PartSpec spec = new PartSpec("reader", new DownstreamRequest(null, "q1"),
                () -> "static-default");
        assertTrue(onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1))).present());
        assertEquals(1, h.server.invocationCount("reader"));

        // 下游转为持续故障，并发发起一批编排请求
        h.server.script("reader", (i, r) -> {
            throw MockDownstreamServer.downstreamError("500 boom");
        });
        int concurrent = 24;
        ExecutorService pool = Executors.newFixedThreadPool(concurrent);
        CountDownLatch ready = new CountDownLatch(concurrent);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<PartResult>> futures = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return onlyPart(orchestrator.aggregate(List.of(spec), Duration.ofSeconds(2)));
            }));
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        for (Future<PartResult> f : futures) {
            PartResult part = f.get(10, TimeUnit.SECONDS);
            assertEquals(PartSource.STALE_FALLBACK, part.source(), "并发下每个失败请求都应回退旧数据");
            assertEquals("good", part.value());
            assertNotNull(part.ageMillis());
        }
        pool.shutdown();

        // 重新安装脚本会重置该下游计数器；并发期间每次请求恰好打下游一次（maxAttempts=1），
        // 回退路径绝不额外调用下游
        assertEquals(concurrent, h.server.invocationCount("reader"),
                "回退不得把下游再打一次");
        // 失败结果不进缓存：缓存里仍只有最初那 1 条成功结果
        assertEquals(1, caches.stats().get("reader").size(),
                "失败结果不得被缓存，计数与内容一致");
        log.info("[reader] 并发={} 全部=旧数据回退 下游调用={}次(无重复) 缓存占用={}/{}(失败未入缓存)",
                concurrent, h.server.invocationCount("reader"),
                caches.stats().get("reader").size(), capacity);
    }

    @Test
    void existingSemanticsUnchangedWhenFallbackEnabled() {
        TestFixtures.Harness h = harness();
        readPolicy(h, "healthy");
        readPolicy(h, "broken");
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport(), caches(h, 8), h.rules);

        h.server.script("healthy", (i, r) -> DownstreamResponse.ok("good-data"));
        h.server.script("broken", (i, r) -> {
            throw MockDownstreamServer.downstreamError("500 boom");
        });

        AggregatedResponse response = orchestrator.aggregate(List.of(
                new PartSpec("healthy", new DownstreamRequest(null, "q1"), () -> "h-default"),
                new PartSpec("broken", new DownstreamRequest(null, "q2"), () -> "b-default")),
                Duration.ofSeconds(2));

        // 整体是否完整、是否降级、缺了哪些部分：语义与之前完全一致
        assertFalse(response.complete());
        assertTrue(response.degraded());
        assertEquals(1, response.missing().size());
        assertTrue(response.missing().get(0).contains("broken"));
        assertTrue(response.parts().get(0).present());
        assertEquals(PartSource.LIVE, response.parts().get(0).source());
        assertFalse(response.parts().get(1).present());
        assertEquals(PartSource.STATIC_FALLBACK, response.parts().get(1).source());
        log.info("语义不变: complete={} degraded={} missing={} healthy=实时 broken=静态兜底",
                response.complete(), response.degraded(), response.missing());
    }
}
