package com.github.xingjianluo.downstreamcallresiliencejava.tests.fallback;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.CachedFallback;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCache;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheStats;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackPolicy;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.tests.support.MutableClock;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 回退缓存单元场景：
 * <ul>
 *   <li>容量上限与 LRU 淘汰规则；</li>
 *   <li>有效期边界：未过期可回退，到期即不可回退；</li>
 *   <li>同键覆盖刷新新旧程度；</li>
 *   <li>并发压力下内容与计数一致、容量不被突破。</li>
 * </ul>
 */
class FallbackCacheTest {

    private static final Logger log = LoggerFactory.getLogger(FallbackCacheTest.class);

    private static DownstreamRequest req(String payload) {
        return new DownstreamRequest(null, payload);
    }

    @Test
    void missWhenNeverSucceeded() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", new FallbackPolicy(4, Duration.ofSeconds(30)), clock);

        assertNull(cache.get(req("q1")), "从未成功过的下游不得有旧数据可回退");
        assertEquals(0, cache.stats().size());
        log.info("[svc] 回退查找=未命中 依据=从未成功过，无旧数据");
    }

    @Test
    void hitReturnsValueWithAge() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", new FallbackPolicy(4, Duration.ofSeconds(30)), clock);

        cache.put(req("q1"), "v1");
        clock.advanceMillis(5_000);

        CachedFallback hit = cache.get(req("q1"));
        assertNotNull(hit);
        assertEquals("v1", hit.value());
        assertEquals(5_000, hit.ageMillis(), "新鲜度必须标明数据是多久之前的");
        log.info("[svc] 回退查找=命中 值=v1 数据年龄={}ms", hit.ageMillis());
    }

    @Test
    void expiredEntryCannotBeUsedAsFallback() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", new FallbackPolicy(4, Duration.ofSeconds(10)), clock);

        cache.put(req("q1"), "v1");
        clock.advanceMillis(9_999);
        assertNotNull(cache.get(req("q1")), "有效期内必须可回退");

        clock.advanceMillis(1); // 恰好到达 TTL 边界
        assertNull(cache.get(req("q1")), "到达有效期即过期，不得再当有效数据回退");
        assertEquals(0, cache.stats().size(), "过期条目必须被移除，不得占用容量");
        log.info("[svc] 回退查找=未命中 依据=旧数据已过期(TTL=10000ms)，改用静态兜底");
    }

    @Test
    void capacityExceededEvictsLeastRecentlyUsed() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", new FallbackPolicy(3, Duration.ofMinutes(5)), clock);

        cache.put(req("a"), "va");
        cache.put(req("b"), "vb");
        cache.put(req("c"), "vc");
        // 访问 a，使 b 成为最久未使用
        assertNotNull(cache.get(req("a")));

        cache.put(req("d"), "vd"); // 超容量：淘汰 b

        assertEquals(3, cache.stats().size(), "条数不得超过容量上限");
        assertNull(cache.get(req("b")), "最久未使用的条目必须被按规则淘汰");
        assertNotNull(cache.get(req("a")));
        assertNotNull(cache.get(req("c")));
        assertNotNull(cache.get(req("d")));
        log.info("[svc] 容量=3 写入第4条 淘汰=最久未使用(b) 留存=a,c,d 计数={}", cache.stats().size());
    }

    @Test
    void putSameKeyOverwritesAndRefreshesAge() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", new FallbackPolicy(4, Duration.ofSeconds(30)), clock);

        cache.put(req("q1"), "old");
        clock.advanceMillis(8_000);
        cache.put(req("q1"), "new");

        assertEquals(1, cache.stats().size(), "同键覆盖不得重复计数");
        CachedFallback hit = cache.get(req("q1"));
        assertNotNull(hit);
        assertEquals("new", hit.value());
        assertEquals(0, hit.ageMillis(), "覆盖后新旧程度从最近一次成功重新计算");
        log.info("[svc] 同键覆盖 值=new 数据年龄=0ms 计数=1");
    }

    @Test
    void statsReflectContentExactly() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", new FallbackPolicy(10, Duration.ofSeconds(5)), clock);

        cache.put(req("a"), "va");
        cache.put(req("b"), "vb");
        FallbackCacheStats stats = cache.stats();
        assertEquals(2, stats.size());
        assertEquals(10, stats.capacity());
        assertEquals(5_000, stats.ttlMillis());

        clock.advanceMillis(6_000); // 全部过期
        assertEquals(0, cache.stats().size(), "过期后计数必须随内容一起清零，不得对不上");
        log.info("[svc] 占用快照 size=2/10 ttl=5000ms；过期后 size=0（内容与计数一致）");
    }

    @Test
    void concurrentPutsNeverExceedCapacityAndCountStaysConsistent() throws Exception {
        MutableClock clock = new MutableClock();
        int capacity = 16;
        FallbackCache cache = new FallbackCache("svc",
                new FallbackPolicy(capacity, Duration.ofMinutes(10)), clock);

        int threads = 32;
        int putsPerThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int thread = t;
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < putsPerThread; i++) {
                    DownstreamRequest key = req("k" + ((thread * putsPerThread + i) % 64));
                    cache.put(key, "v" + thread + "-" + i);
                    // 交替读取，模拟并发回退查找
                    cache.get(key);
                    assertTrue(cache.stats().size() <= capacity,
                            "任何时刻条数都不得超过容量上限");
                }
            }));
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        for (Future<?> f : futures) {
            f.get(15, TimeUnit.SECONDS);
        }
        pool.shutdown();

        FallbackCacheStats stats = cache.stats();
        assertEquals(capacity, stats.size(), "并发写入后必须恰好停在容量上限，计数与内容一致");
        log.info("[svc] 并发压力={}线程x{}次写入 最终占用={}/{} 容量未被突破、计数一致",
                threads, putsPerThread, stats.size(), capacity);
    }
}
