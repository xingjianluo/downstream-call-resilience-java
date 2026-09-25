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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 熔断基本场景：连续失败达到阈值后打开，打开期间请求快速失败；
 * 成功清零连续失败计数；熔断器按下游独立、互不连带。
 */
class CircuitBreakerTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerTest.class);

    private CircuitBreaker newBreaker(String name) {
        MutableClock clock = new MutableClock();
        ResiliencePolicy policy = new ResiliencePolicy(name, Duration.ofMillis(100), 1,
                Duration.ZERO, 3, Duration.ofSeconds(5), 1, OperationKind.READ);
        return new CircuitBreaker(name, policy, clock);
    }

    @Test
    void opensAfterThresholdAndFastFails() {
        CircuitBreaker breaker = newBreaker("payment");
        log.info("[payment] 初始: {}", breaker.snapshot());

        for (int i = 1; i <= 2; i++) {
            CircuitBreaker.Permit permit = breaker.tryAcquire();
            assertNotNull(permit, "阈值内应放行");
            breaker.recordFailure(permit, FailureCategory.DOWNSTREAM_ERROR);
            assertEquals(CircuitState.CLOSED, breaker.state(),
                    "第 " + i + " 次失败未达阈值，应仍为 CLOSED");
            log.info("[payment] 第 {} 次失败后: {} 判定=未达阈值保持CLOSED", i, breaker.snapshot());
        }

        CircuitBreaker.Permit third = breaker.tryAcquire();
        assertNotNull(third);
        breaker.recordFailure(third, FailureCategory.DOWNSTREAM_ERROR);
        assertEquals(CircuitState.OPEN, breaker.state(), "连续 3 次失败后必须 OPEN");
        log.info("[payment] 第 3 次失败后: {} 判定=达到阈值迁移OPEN", breaker.snapshot());

        // 打开期间：不等待，直接拿不到许可（快速失败）
        long start = System.nanoTime();
        CircuitBreaker.Permit rejected = breaker.tryAcquire();
        long blockedMicros = (System.nanoTime() - start) / 1_000;
        assertNull(rejected, "OPEN 状态必须快速失败");
        log.info("[payment] OPEN 期间请求判定=CIRCUIT_OPEN快速失败 阻塞耗时={}us", blockedMicros);
    }

    @Test
    void successResetsConsecutiveFailures() {
        CircuitBreaker breaker = newBreaker("profile");
        CircuitBreaker.Permit p1 = breaker.tryAcquire();
        breaker.recordFailure(p1, FailureCategory.CONNECTION_FAILED);
        CircuitBreaker.Permit p2 = breaker.tryAcquire();
        breaker.recordSuccess(p2);
        log.info("[profile] 失败1次后成功: {} 判定=连续失败清零", breaker.snapshot());

        // 清零后再失败 2 次不应熔断
        for (int i = 0; i < 2; i++) {
            CircuitBreaker.Permit p = breaker.tryAcquire();
            breaker.recordFailure(p, FailureCategory.TIMEOUT);
        }
        assertEquals(CircuitState.CLOSED, breaker.state(),
                "成功清零后，失败计数从头累计，状态须准确");
        log.info("[profile] 再失败2次后: {} 判定=计数准确未误熔断", breaker.snapshot());
    }

    @Test
    void breakersAreIsolatedPerDownstream() {
        CircuitBreaker a = newBreaker("service-a");
        CircuitBreaker b = newBreaker("service-b");

        for (int i = 0; i < 3; i++) {
            CircuitBreaker.Permit p = a.tryAcquire();
            a.recordFailure(p, FailureCategory.DOWNSTREAM_ERROR);
        }
        assertEquals(CircuitState.OPEN, a.state());
        assertEquals(CircuitState.CLOSED, b.state(),
                "A 熔断不得连带健康的 B");
        assertNotNull(b.tryAcquire(), "健康下游 B 必须继续放行");
        log.info("[service-a]={} [service-b]={} 判定=熔断按下游隔离互不连带",
                a.snapshot(), b.snapshot());
    }
}
