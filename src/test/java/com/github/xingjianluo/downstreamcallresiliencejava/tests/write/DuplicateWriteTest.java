package com.github.xingjianluo.downstreamcallresiliencejava.tests.write;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重复写场景：
 * <ul>
 *   <li>幂等写首次超时，重试由下游按幂等键去重，业务写只执行一次；</li>
 *   <li>同一幂等键的后续调用收到"重复响应"（duplicate=true），不再执行写；</li>
 *   <li>同一幂等键并发调用，写也只执行一次；</li>
 *   <li>无幂等键的非幂等写不享受该保护。</li>
 * </ul>
 */
class DuplicateWriteTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateWriteTest.class);

    private TestFixtures.Harness harness() {
        return new TestFixtures.Harness();
    }

    @Test
    void idempotentWriteTimeoutRetryExecutesOnceWhenFirstConfirmedLate() {
        TestFixtures.Harness h = harness();
        String name = "idem-write-late";
        h.withPolicy(TestFixtures.policy(name, Duration.ofMillis(200), 3, 10,
                Duration.ofHours(1), 1, OperationKind.IDEMPOTENT_WRITE));
        // 首次尝试：客户端 200ms 超时，但服务端在 300ms 时完成写并确认（响应迟到）；
        // 重试在首次释放幂等键锁后（自己的 200ms 预算内）拿到重复响应回放。
        h.server.script(name, (index, request) -> {
            if (index == 0) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return DownstreamResponse.confirmedWrite("created-0");
        });

        CallOutcome<DownstreamResponse> outcome = h.client.execute(name,
                () -> h.server.transport().invoke(name,
                        new DownstreamRequest("key-1001", "payload")));

        assertTrue(outcome.success(), "重试应通过幂等键拿到首次结果: " + outcome.reason());
        assertEquals(2, outcome.attempts(), "首次超时允许重试一次");
        assertEquals(1, h.server.invocationCount(name),
                "服务端写只执行一次：首次迟到确认已登记幂等键，重试直接回放");
        assertTrue(outcome.value().duplicate(), "重试拿到的必须是重复响应，而不是第二次写");
        log.info("[{}] 结果=成功 尝试=2 写执行=1 重试响应duplicate=true 判定=迟到确认+幂等去重不重复写",
                name);
    }

    @Test
    void idempotentWriteTimeoutRetryWhenFirstNeverConfirmed() {
        TestFixtures.Harness h = harness();
        String name = "idem-write-never";
        h.withPolicy(TestFixtures.policy(name, Duration.ofMillis(100), 3, 10,
                Duration.ofHours(1), 1, OperationKind.IDEMPOTENT_WRITE));
        // 首次在确认前被超时中断：中断即抛 TIMEOUT，首次写绝不确认；第二次才确认。
        h.server.script(name, (index, request) -> {
            if (index == 0) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw MockDownstreamServer.timeoutFailure("首次写未确认即被超时中断");
            }
            return DownstreamResponse.confirmedWrite("created-1");
        });

        CallOutcome<DownstreamResponse> outcome = h.client.execute(name,
                () -> h.server.transport().invoke(name,
                        new DownstreamRequest("key-1002", "payload")));

        assertTrue(outcome.success(), outcome.reason());
        assertEquals(2, outcome.attempts());
        assertEquals(2, h.server.invocationCount(name), "首次未确认，传输层确有两次尝试");
        assertFalse(outcome.value().duplicate());
        log.info("[{}] 结果=成功 尝试=2 写执行=2(首次未确认未生效) 判定=幂等键保证最终只有一次生效",
                name);
    }

    @Test
    void confirmedIdempotentWriteIsReplayedOnSameKey() {
        TestFixtures.Harness h = harness();
        String name = "idem-store";
        h.withPolicy(TestFixtures.policy(name, Duration.ofSeconds(1), 3, 10,
                Duration.ofHours(1), 1, OperationKind.IDEMPOTENT_WRITE));
        h.server.script(name, (index, request) -> DownstreamResponse.confirmedWrite("stored"));

        DownstreamRequest first = new DownstreamRequest("order-777", "data");
        DownstreamResponse r1 = h.server.transport().invoke(name, first);
        DownstreamResponse r2 = h.server.transport().invoke(name, first);
        DownstreamResponse r3 = h.server.transport().invoke(name, first);

        assertFalse(r1.duplicate());
        assertTrue(r2.duplicate(), "相同幂等键的第二次响应必须标记为重复响应");
        assertTrue(r3.duplicate());
        assertEquals(1, h.server.invocationCount(name),
                "业务写只允许执行一次，重复调用回放首次结果");
        assertEquals(r1.body(), r2.body());
        log.info("[{}] 首次duplicate=false 第2/3次duplicate=true 写执行=1 判定=幂等键去重重复响应",
                name);
    }

    @Test
    void concurrentCallsWithSameKeyExecuteWriteOnce() throws Exception {
        TestFixtures.Harness h = harness();
        String name = "idem-concurrent";
        h.withPolicy(TestFixtures.policy(name, Duration.ofSeconds(2), 1, 10,
                Duration.ofHours(1), 1, OperationKind.IDEMPOTENT_WRITE));
        h.server.script(name, (index, request) -> DownstreamResponse.confirmedWrite("once"));

        int callers = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<DownstreamResponse> responses = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                DownstreamResponse response = h.server.transport().invoke(name,
                        new DownstreamRequest("concurrent-key", "data"));
                synchronized (responses) {
                    responses.add(response);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long duplicates = responses.stream().filter(DownstreamResponse::duplicate).count();
        assertEquals(callers, responses.size());
        assertEquals(1, h.server.invocationCount(name),
                "并发相同幂等键，写必须只执行一次");
        assertEquals(callers - 1, duplicates, "其余调用全部拿到重复响应");
        log.info("[{}] 并发调用={} 写执行=1 重复响应={} 判定=并发下去重依然成立",
                name, callers, duplicates);
    }

    @Test
    void nonIdempotentWriteHasNoDedupeProtection() {
        TestFixtures.Harness h = harness();
        String name = "raw-write";
        h.withPolicy(TestFixtures.policy(name, Duration.ofSeconds(1), 2, 10,
                Duration.ofHours(1), 1, OperationKind.NON_IDEMPOTENT_WRITE));
        h.server.script(name, (index, request) -> DownstreamResponse.confirmedWrite("done"));

        // 没有幂等键：若上游执意重复发起（模拟上游bug），下游无法识别重复，会执行多次；
        // 弹性层的职责是在判定上禁止这种重试（见 RetryAndTimeoutTest）。
        h.server.transport().invoke(name, new DownstreamRequest(null, "x"));
        h.server.transport().invoke(name, new DownstreamRequest(null, "x"));
        assertEquals(2, h.server.invocationCount(name));
        log.info("[{}] 无幂等键调用2次 写执行=2 判定=非幂等写需由弹性层禁止重试而非依赖下游去重",
                name);
    }
}
