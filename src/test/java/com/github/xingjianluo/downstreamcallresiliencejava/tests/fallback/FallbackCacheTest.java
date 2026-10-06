package com.github.xingjianluo.downstreamcallresiliencejava.tests.fallback;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCache;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackEntry;
import com.github.xingjianluo.downstreamcallresiliencejava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 回退缓存：容量上限（LRU）、有效期（边界）、并发下内容与计数一致。
 */
class FallbackCacheTest {

    private static final Logger log = LoggerFactory.getLogger(FallbackCacheTest.class);

    @Test
    void storesAndReturnsRecentSuccessWithAge() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", 4, Duration.ofMillis(100), clock);

        cache.store("k", "v1");
        clock.advanceMillis(30);
        FallbackEntry hit = cache.findValid("k");
        assertNotNull(hit, "未过期的最近成功结果必须可回退");
        assertEquals("v1", hit.value());
        assertEquals(30, clock.millis() - hit.capturedAt(), "新鲜度（年龄）必须可计算");
        assertNull(cache.findValid("never"), "从未成功过的键不得返回旧数据");
        log.info("旧数据命中 age=30ms ttl=100ms 来源=该下游最近一次成功结果");
    }

    @Test
    void expiryBoundaryIsInclusiveTtlAndExpiredNotServed() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", 4, Duration.ofMillis(100), clock);
        cache.store("k", "v");

        clock.advanceMillis(100);
        assertNotNull(cache.findValid("k"), "age == ttl 边界上仍算有效");
        assertEquals(1, cache.size(), "边界内条目仍计入占用");

        clock.advanceMillis(1);
        assertNull(cache.findValid("k"), "age > ttl 必须判为过期，不得再回退");
        assertEquals(0, cache.size(), "过期清理后内容与计数必须一致");
        log.info("过期边界: age=100ms 仍有效, age=101ms 过期淘汰且不再回退");
    }

    @Test
    void capacityEvictsLeastRecentlyUsed() {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", 2, Duration.ofHours(1), clock);
        cache.store("a", "1");
        cache.store("b", "2");
        // 访问 a，使 b 成为 LRU
        assertNotNull(cache.findValid("a"));
        cache.store("c", "3");

        assertEquals(2, cache.size(), "容量上限不得超过 2");
        assertNotNull(cache.findValid("a"), "最近访问的 a 必须保留");
        assertNotNull(cache.findValid("c"));
        assertNull(cache.findValid("b"), "LRU 最旧的 b 必须被淘汰");
        log.info("容量=2 淘汰规则=LRU 淘汰=b 保留=[a,c]");
    }

    @Test
    void refreshingKeyDoesNotBlockExpiryOfOtherKeys() {
        // 回归：覆盖刷新某键后，其他真正过期的键仍必须被正确清理（陈旧插入标记场景）
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", 8, Duration.ofMillis(100), clock);
        cache.store("a", "a1");
        cache.store("b", "b1");
        clock.advanceMillis(60);
        cache.store("a", "a2"); // 刷新 a
        clock.advanceMillis(50); // b 的年龄=110 已过期；a2 的年龄=50 仍有效

        assertEquals(1, cache.size(), "b 必须按过期清理，不能被 a 的旧标记挡住");
        FallbackEntry a = cache.findValid("a");
        assertNotNull(a);
        assertEquals("a2", a.value());
        assertNull(cache.findValid("b"));
        log.info("刷新 a 后: b(age=110ms) 过期清理, a(age=50ms) 仍有效, 有效条数=1");
    }

    @Test
    void concurrentDistinctWritesNeverExceedCapacityAndCountMatchesContent() throws Exception {
        MutableClock clock = new MutableClock();
        int capacity = 10;
        int threads = 32;
        int perThread = 20;
        FallbackCache cache = new FallbackCache("svc", capacity, Duration.ofHours(1), clock);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    cache.store("key-" + (base + i), "v");
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        // 并发压力下：占用永不超过容量，且计数与实际可命中的内容完全一致
        assertTrue(cache.size() <= capacity, "有效条数不得超过容量上限: " + cache.size());
        Set<String> reachable = new HashSet<>();
        for (int i = 0; i < threads * perThread; i++) {
            if (cache.findValid("key-" + i) != null) {
                reachable.add("key-" + i);
            }
        }
        assertEquals(cache.size(), reachable.size(), "缓存内容与计数必须对得上");
        log.info("并发写入 {} 线程 x{} 条, 容量={} 实际有效={} 内容与计数一致",
                threads, perThread, capacity, cache.size());
    }

    @Test
    void concurrentSameKeyStoresKeepSingleConsistentEntry() throws Exception {
        MutableClock clock = new MutableClock();
        FallbackCache cache = new FallbackCache("svc", 4, Duration.ofHours(1), clock);
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        Set<String> observed = ConcurrentHashMap.newKeySet();
        for (int t = 0; t < threads; t++) {
            final String value = "v" + t;
            pool.submit(() -> {
                start.await();
                cache.store("same", value);
                FallbackEntry e = cache.findValid("same");
                if (e != null) {
                    observed.add(e.value());
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, cache.size(), "同键并发覆盖后只允许保留一条");
        FallbackEntry now = cache.findValid("same");
        assertNotNull(now);
        assertTrue(observed.contains(now.value()), "当前值必须是某次成功写入的值");
        log.info("同键 32 线程并发覆盖: 有效条数=1 当前值={}", now.value());
    }
}
