package com.github.xingjianluo.downstreamcallresiliencejava.tests.circuit;

import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitBreaker;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitState;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResiliencePolicy;
import com.github.xingjianluo.downstreamcallresiliencejava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发状态迁移场景：
 * <ul>
 *   <li>恰好阈值数量的线程同时上报失败，熔断必须且只打开一次；</li>
 *   <li>打开瞬间大量并发请求全部快速失败，没有"漏网"请求被放行；</li>
 *   <li>打开后继续并发上报的迟到失败不得让计数丢失或重复影响代际。</li>
 * </ul>
 */
class CircuitConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitConcurrencyTest.class);

    private CircuitBreaker breaker(MutableClock clock, int threshold, int halfOpenPermits) {
        ResiliencePolicy policy = new ResiliencePolicy("concurrent-svc", Duration.ofMillis(100), 1,
                Duration.ZERO, threshold, Duration.ofSeconds(60), halfOpenPermits,
                OperationKind.READ);
        return new CircuitBreaker("concurrent-svc", policy, clock);
    }

    @Test
    void concurrentFailuresOpenExactlyOnce() throws Exception {
        int threshold = 20;
        MutableClock clock = new MutableClock();
        CircuitBreaker cb = breaker(clock, threshold, 1);

        int threads = threshold;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger failuresRecorded = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                CircuitBreaker.Permit permit = cb.tryAcquire();
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                cb.recordFailure(permit, FailureCategory.DOWNSTREAM_ERROR);
                failuresRecorded.incrementAndGet();
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(threshold, failuresRecorded.get(), "所有失败都必须被记账，不得丢失");
        assertEquals(CircuitState.OPEN, cb.state(), "阈值数量的并发失败必须恰好迁移到 OPEN");
        log.info("并发失败 {} 次后: {} 判定=并发迁移恰好一次计数不重不漏",
                failuresRecorded.get(), cb.snapshot());
    }

    @Test
    void requestsAfterConcurrentOpenAreAllFastFailed() throws Exception {
        int threshold = 10;
        MutableClock clock = new MutableClock();
        CircuitBreaker cb = breaker(clock, threshold, 1);

        for (int i = 0; i < threshold; i++) {
            CircuitBreaker.Permit p = cb.tryAcquire();
            cb.recordFailure(p, FailureCategory.TIMEOUT);
        }
        assertEquals(CircuitState.OPEN, cb.state());

        int callers = 100;
        ExecutorService pool = Executors.newFixedThreadPool(20);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger fastFailed = new AtomicInteger();
        AtomicInteger passed = new AtomicInteger();
        for (int i = 0; i < callers; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (cb.tryAcquire() == null) {
                    fastFailed.incrementAndGet();
                } else {
                    passed.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(callers, fastFailed.get(), "OPEN 后所有并发请求都必须快速失败");
        assertEquals(0, passed.get(), "不得有任何请求穿透 OPEN 状态");
        log.info("OPEN 后 {} 个并发请求判定=全部CIRCUIT_OPEN快速失败 穿透数={}",
                fastFailed.get(), passed.get());
    }
}
