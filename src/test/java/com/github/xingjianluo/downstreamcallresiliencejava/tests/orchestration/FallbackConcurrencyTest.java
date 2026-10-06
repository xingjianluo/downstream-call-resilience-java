package com.github.xingjianluo.downstreamcallresiliencejava.tests.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCache;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.DataSource;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.AggregatedResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartResult;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.tests.support.TestFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 回退能力在并发压力下的边界：
 * <ul>
 *   <li>每个下游的每次逻辑聚合只真正调用一次（回退不再补打下游）；</li>
 *   <li>大量不同键成功后缓存占用永不超过容量，且内容与计数一致；</li>
 *   <li>失败结果不会进入缓存。</li>
 * </ul>
 */
class FallbackConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(FallbackConcurrencyTest.class);

    @Test
    void concurrentAggregationsCapCapacityKeepCountConsistentAndDoNotCacheFailures()
            throws Exception {
        TestFixtures.Harness h = new TestFixtures.Harness();
        int capacity = 8;
        h.withPolicy(TestFixtures.policyWithFallback("multi", Duration.ofSeconds(1), 1, 1000,
                Duration.ofHours(1), 1, OperationKind.READ, capacity, Duration.ofHours(1)));
        Orchestrator orch = h.orchestrator();

        // 全部成功：每个键不同，模拟大量并发读聚合
        h.server.script("multi", (i, r) -> DownstreamResponse.ok("body-of-" + r.payload()));

        int threads = 24;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger liveCount = new AtomicInteger();
        List<Throwable> errors = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        String payload = "k" + (base + i);
                        PartSpec spec = new PartSpec("multi",
                                new DownstreamRequest(null, payload), () -> "def");
                        AggregatedResponse resp = orch.aggregate(List.of(spec), Duration.ofSeconds(2));
                        PartResult part = resp.parts().get(0);
                        if (part.source() == DataSource.LIVE) {
                            liveCount.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    synchronized (errors) {
                        errors.add(e);
                    }
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertTrue(errors.isEmpty(), "并发聚合不应抛异常: " + errors);

        FallbackCache cache = h.fallbacks.get("multi");
        int size = cache.size();
        assertTrue(size <= capacity, "并发写入后占用不得超过容量 " + capacity + ": " + size);
        // 内容与计数一致：枚举全部逻辑键，能命中的条数必须等于 size
        Set<String> reachable = new HashSet<>();
        for (int i = 0; i < threads * perThread; i++) {
            if (cache.findValid("payload:k" + i) != null) {
                reachable.add("payload:k" + i);
            }
        }
        assertEquals(size, reachable.size(), "缓存内容与计数必须对得上");
        // 每个逻辑键对应一次真实下游调用，不多不少（LIVE 次数 == 真实进入服务器次数）
        assertEquals(liveCount.get(), h.server.invocationCount("multi"),
                "每次实时成功对应恰好一次下游调用；回退路径不得再打下游");
        log.info("并发聚合: {} 线程 x{} 实时成功={} 下游真实调用={} 缓存占用={}/{} 内容计数一致",
                threads, perThread, liveCount.get(), h.server.invocationCount("multi"),
                size, capacity);
    }

    @Test
    void staleFallbackUnderConcurrencyIssuesNoExtraDownstreamCall() throws Exception {
        TestFixtures.Harness h = new TestFixtures.Harness();
        h.withPolicy(TestFixtures.policyWithFallback("flaky", Duration.ofSeconds(1), 1, 1000,
                Duration.ofHours(1), 1, OperationKind.READ, 8, Duration.ofHours(1)));
        Orchestrator orch = h.orchestrator();

        // 固定请求：首次成功缓存，之后全部失败 -> 全部应回退同一条旧数据
        AtomicInteger attempts = new AtomicInteger();
        h.server.script("flaky", (i, r) -> {
            attempts.incrementAndGet();
            if (i == 0) {
                return DownstreamResponse.ok("only-good");
            }
            throw MockDownstreamServer.downstreamError("500");
        });
        PartSpec spec = new PartSpec("flaky", new DownstreamRequest(null, "same-q"),
                () -> "static-def");

        // 先建立旧数据
        assertEquals(DataSource.LIVE,
                orch.aggregate(List.of(spec), Duration.ofSeconds(2)).parts().get(0).source());

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger stale = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                start.await();
                PartResult part = orch.aggregate(List.of(spec), Duration.ofSeconds(2)).parts().get(0);
                if (part.source() == DataSource.STALE && "only-good".equals(part.value())) {
                    stale.incrementAndGet();
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(threads, stale.get(), "全部并发失败请求都必须回退到同一条旧数据");
        // 每个并发请求各真实尝试一次（maxAttempts=1），回退查找不产生任何额外下游调用；
        // 即真实下游调用次数 == 1（首次成功）+ threads（失败尝试），而非 2 倍。
        int expected = 1 + threads;
        assertEquals(expected, h.server.invocationCount("flaky"),
                "回退是纯内存查找，不得为回退再打一次下游");
        log.info("并发旧数据回退: {} 个失败请求全部 STALE 命中同一条旧数据, 下游真实调用={}（=1+{}，无回退补打）",
                threads, h.server.invocationCount("flaky"), threads);
    }
}
