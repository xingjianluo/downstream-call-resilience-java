package com.github.xingjianluo.downstreamcallresiliencejava.tests.retry;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CallOutcome;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.tests.support.TestFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 超时 / 重试 / 失败分类场景：
 * 连接失败、下游错误、超时归入可区分类别；重试次数准确；
 * 非幂等写超时只执行一次；达到熔断阈值后快速失败。
 */
class RetryAndTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndTimeoutTest.class);

    private TestFixtures.Harness harness() {
        return new TestFixtures.Harness();
    }

    @Test
    void connectionFailureIsRetriedAndCategorized() {
        TestFixtures.Harness h = harness();
        String name = "conn-svc";
        h.withPolicy(TestFixtures.policy(name, Duration.ofSeconds(1), 3, 10,
                Duration.ofHours(1), 1, OperationKind.READ));
        h.server.script(name, (index, request) -> {
            throw MockDownstreamServer.connectionFailure("connect refused");
        });

        CallOutcome<DownstreamResponse> outcome = h.client.execute(name,
                () -> h.server.transport().invoke(name, new DownstreamRequest(null, "r")));

        assertFalse(outcome.success());
        assertEquals(FailureCategory.CONNECTION_FAILED, outcome.category(),
                "连接失败必须归为 CONNECTION_FAILED");
        assertEquals(3, outcome.attempts(), "连接失败可安全重试，应用尽 3 次尝试");
        log.info("[{}] 结果={} 类别={} 尝试={} 依据={}", name, "失败", outcome.category(),
                outcome.attempts(), outcome.reason());
    }

    @Test
    void downstreamErrorIsCategorizedAndRetriedThenSucceeds() {
        TestFixtures.Harness h = harness();
        String name = "err-svc";
        h.withPolicy(TestFixtures.policy(name, Duration.ofSeconds(1), 3, 10,
                Duration.ofHours(1), 1, OperationKind.READ));
        h.server.script(name, (index, request) -> {
            if (index < 2) {
                throw MockDownstreamServer.downstreamError("500 internal");
            }
            return DownstreamResponse.ok("recovered");
        });

        CallOutcome<DownstreamResponse> outcome = h.client.execute(name,
                () -> h.server.transport().invoke(name, new DownstreamRequest(null, "r")));

        assertTrue(outcome.success());
        assertEquals(3, outcome.attempts(), "前两次下游错误重试，第三次成功");
        assertEquals(FailureCategory.SUCCESS, outcome.category());
        log.info("[{}] 结果={} 类别={} 尝试={} 值={}", name, "成功", outcome.category(),
                outcome.attempts(), outcome.value().body());
    }

    @Test
    void realTimeoutIsCategorizedAsTimeout() {
        TestFixtures.Harness h = harness();
        String name = "slow-svc";
        h.withPolicy(TestFixtures.policy(name, Duration.ofMillis(80), 2, 10,
                Duration.ofHours(1), 1, OperationKind.READ));
        h.server.script(name, (index, request) -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return DownstreamResponse.ok("late");
        });

        long start = System.nanoTime();
        CallOutcome<DownstreamResponse> outcome = h.client.execute(name,
                () -> h.server.transport().invoke(name, new DownstreamRequest(null, "r")));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertFalse(outcome.success());
        assertEquals(FailureCategory.TIMEOUT, outcome.category(),
                "真实超时必须归为 TIMEOUT，而不是下游错误");
        assertEquals(2, outcome.attempts());
        assertTrue(elapsedMillis < 600,
                "只读超时可重试，但总耗时应约等于两次超时之和: " + elapsedMillis + "ms");
        log.info("[{}] 结果=失败 类别=TIMEOUT 尝试={} 总耗时={}ms 依据={}", name,
                outcome.attempts(), elapsedMillis, outcome.reason());
    }

    @Test
    void nonIdempotentWriteTimeoutIsNeverRetried() {
        TestFixtures.Harness h = harness();
        String name = "pay-write";
        h.withPolicy(TestFixtures.policy(name, Duration.ofMillis(80), 5, 10,
                Duration.ofHours(1), 1, OperationKind.NON_IDEMPOTENT_WRITE));
        AtomicInteger executions = new AtomicInteger();
        h.server.script(name, (index, request) -> {
            executions.incrementAndGet();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return DownstreamResponse.confirmedWrite("paid");
        });

        CallOutcome<DownstreamResponse> outcome = h.client.execute(name,
                () -> h.server.transport().invoke(name, new DownstreamRequest(null, "charge")));

        assertEquals(FailureCategory.TIMEOUT, outcome.category());
        assertEquals(1, outcome.attempts(),
                "非幂等写超时副作用未知，禁止重试，只能尝试 1 次");
        assertEquals(1, executions.get(), "写逻辑在服务器侧也只能执行 1 次");
        assertTrue(h.rules.isSideEffectAmbiguous(OperationKind.NON_IDEMPOTENT_WRITE,
                FailureCategory.TIMEOUT, false),
                "降级路径必须一致地判定该写副作用状态未知");
        log.info("[{}] 结果=失败 类别=TIMEOUT 尝试=1 写执行={} 降级判定=副作用未知 依据={}",
                name, executions.get(), outcome.reason());
    }

    @Test
    void confirmedWriteFollowedByErrorIsNotRetried() {
        TestFixtures.Harness h = harness();
        String name = "confirm-write";
        h.withPolicy(TestFixtures.policy(name, Duration.ofSeconds(1), 5, 10,
                Duration.ofHours(1), 1, OperationKind.NON_IDEMPOTENT_WRITE));
        h.server.script(name, (index, request) -> {
            throw MockDownstreamServer.confirmedThenError("写已落库但响应通道错误");
        });

        CallOutcome<DownstreamResponse> outcome = h.client.execute(name,
                () -> h.server.transport().invoke(name, new DownstreamRequest(null, "w")));

        assertEquals(FailureCategory.DOWNSTREAM_ERROR, outcome.category());
        assertEquals(1, outcome.attempts(),
                "下游已确认写生效后返回错误，绝不允许重试导致重复写");
        assertEquals(1, h.server.invocationCount(name));
        assertFalse(h.rules.isSideEffectAmbiguous(OperationKind.NON_IDEMPOTENT_WRITE,
                FailureCategory.DOWNSTREAM_ERROR, true),
                "已确认生效的写，副作用不模糊");
        log.info("[{}] 结果=失败 类别=DOWNSTREAM_ERROR 尝试=1 写执行=1 判定=已确认生效禁止重试", name);
    }
}
