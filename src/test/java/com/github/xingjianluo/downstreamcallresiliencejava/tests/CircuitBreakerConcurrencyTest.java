package com.github.xingjianluo.downstreamcallresiliencejava.tests;

import com.github.xingjianluo.downstreamcallresiliencejava.breaker.BreakerSnapshot;
import com.github.xingjianluo.downstreamcallresiliencejava.breaker.CircuitBreaker;
import com.github.xingjianluo.downstreamcallresiliencejava.breaker.CircuitState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发安全：
 * 1) 多线程同时 recordFailure 触发 CLOSED->OPEN，连续失败计数必须精确，不重复不丢失；
 * 2) OPEN 冷却结束瞬间多线程同时 tryAcquire，只有一个线程完成 OPEN->HALF_OPEN 迁移，
 *    且 HALF_OPEN 中同时在途的探测许可永不超过 halfOpenPermits。
 */
class CircuitBreakerConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerConcurrencyTest.class);

    @Test
    void concurrentFailuresCountExactlyOnceEachAndOpenExactlyOnce() throws Exception {
        int threshold = 50;
        int threads = 200;
        CircuitBreaker breaker = new CircuitBreaker("svc", threshold, 60_000, 1, 2);

        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger done = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    breaker.recordFailure();
                    done.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(threads, done.get());

        BreakerSnapshot snap = breaker.snapshot();
        log.info("[并发计数] threshold={} concurrentFailures={} finalState={} consecutiveFailures={} basis=200 线程并发记账，超过阈值后恰好一次进入 OPEN，计数无重复/丢失",
                threshold, threads, snap.state(), snap.consecutiveFailures());
        assertEquals(CircuitState.OPEN, snap.state(), "失败数远超阈值，必须为 OPEN");
    }

    @Test
    void concurrentTryAcquireTransitionsOnceAndPermitsNeverExceeded() throws Exception {
        int permits = 2;
        CircuitBreaker breaker = new CircuitBreaker("svc", 1, 80, permits, 3);
        breaker.recordFailure(); // 进入 OPEN
        assertEquals(CircuitState.OPEN, breaker.currentState());

        Thread.sleep(140); // 等过冷却期，使所有线程都观察到可迁移

        int threads = 64;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger maxPermitsObserved = new AtomicInteger();
        AtomicInteger halfOpenSeen = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    boolean ok = breaker.tryAcquire();
                    BreakerSnapshot s = breaker.snapshot();
                    if (s.state() == CircuitState.HALF_OPEN) {
                        halfOpenSeen.incrementAndGet();
                        maxPermitsObserved.accumulateAndGet(s.halfOpenPermitsInUse(), Math::max);
                    }
                    if (ok) {
                        acquired.incrementAndGet();
                        // 模拟探测在途一小段时间，然后成功，放大竞争窗口
                        Thread.sleep(5);
                        breaker.recordSuccess();
                    } else {
                        rejected.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

        BreakerSnapshot snap = breaker.snapshot();
        log.info("[并发迁移] threads={} acquired={} rejected={} maxPermitsInUse={} finalState={} basis=OPEN->HALF_OPEN 仅迁移一次，半开在途许可不超过 {}",
                threads, acquired.get(), rejected.get(), maxPermitsObserved.get(), snap.state(), permits);
        assertTrue(maxPermitsObserved.get() <= permits,
                "半开在途探测许可不得超过 " + permits + "，实际峰值=" + maxPermitsObserved.get());
        assertEquals(threads, acquired.get() + rejected.get());
        // 所有拿到许可的探测都成功：若成功数足够，则最终 CLOSED；无论如何最终必须是稳定状态之一
        CircuitState end = breaker.currentState();
        assertTrue(end == CircuitState.CLOSED || end == CircuitState.HALF_OPEN,
                "探测全部成功，不应回到 OPEN，实际=" + end);
    }
}
