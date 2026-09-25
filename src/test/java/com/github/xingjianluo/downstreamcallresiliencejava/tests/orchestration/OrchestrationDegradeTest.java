package com.github.xingjianluo.downstreamcallresiliencejava.tests.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.AggregatedResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartResult;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.PartSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.tests.support.TestFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 请求级降级与部分结果场景：
 * <ul>
 *   <li>某下游不可用时整体响应仍在约定时限内返回；</li>
 *   <li>降级结果标明缺失部分与失败类别、判定依据、尝试次数；</li>
 *   <li>健康下游结果完整，不受故障下游连带；</li>
 *   <li>整体时限到期时慢下游按 TIMEOUT 降级。</li>
 * </ul>
 */
class OrchestrationDegradeTest {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationDegradeTest.class);

    private TestFixtures.Harness harness() {
        return new TestFixtures.Harness();
    }

    private void standardPolicies(TestFixtures.Harness h) {
        h.withPolicy(TestFixtures.policy("healthy", Duration.ofMillis(200), 2, 10,
                Duration.ofHours(1), 1, OperationKind.READ));
        h.withPolicy(TestFixtures.policy("broken", Duration.ofMillis(200), 3, 3,
                Duration.ofHours(1), 1, OperationKind.READ));
        h.withPolicy(TestFixtures.policy("slow", Duration.ofMillis(500), 2, 10,
                Duration.ofHours(1), 1, OperationKind.READ));
    }

    private List<PartSpec> threeParts() {
        return List.of(
                new PartSpec("healthy", new DownstreamRequest(null, "q1"),
                        () -> "healthy-fallback"),
                new PartSpec("broken", new DownstreamRequest(null, "q2"),
                        () -> "broken-default"),
                new PartSpec("slow", new DownstreamRequest(null, "q3"),
                        () -> "slow-default"));
    }

    @Test
    void oneDownstreamDownReturnsPartialResultOnTime() {
        TestFixtures.Harness h = harness();
        standardPolicies(h);
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport());

        h.server.script("healthy", (i, r) -> DownstreamResponse.ok("good-data"));
        h.server.script("broken", (i, r) -> {
            throw MockDownstreamServer.downstreamError("500 boom");
        });
        h.server.script("slow", (i, r) -> DownstreamResponse.ok("slow-data"));

        long start = System.nanoTime();
        AggregatedResponse response = orchestrator.aggregate(threeParts(), Duration.ofSeconds(2));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertFalse(response.complete(), "存在缺失分片，complete 必须为 false");
        assertTrue(response.degraded());
        assertTrue(elapsedMillis < 2_000, "整体必须在约定时限内返回: " + elapsedMillis + "ms");

        PartResult healthy = response.parts().get(0);
        PartResult broken = response.parts().get(1);
        PartResult slow = response.parts().get(2);

        assertTrue(healthy.present());
        assertEquals("good-data", healthy.value());
        assertFalse(broken.present());
        assertTrue(broken.degraded());
        assertEquals(FailureCategory.DOWNSTREAM_ERROR, broken.category(),
                "失败类别必须可区分");
        assertEquals(3, broken.attempts(), "重试耗尽后才降级，尝试次数必须准确");
        assertEquals("broken-default", broken.value(), "降级值必须回填");
        assertNotNull(broken.reason());
        assertTrue(slow.present(), "健康/慢但成功的下游不受连带");

        assertEquals(1, response.missing().size());
        assertTrue(response.missing().get(0).contains("broken"));
        log.info("聚合结果 complete={} elapsedMs={} 缺失={} | healthy=完整 broken=降级({},{}) slow=完整",
                response.complete(), elapsedMillis, response.missing(),
                broken.category(), broken.reason());
    }

    @Test
    void overallDeadlineCutsOffSlowDownstream() {
        TestFixtures.Harness h = harness();
        standardPolicies(h);
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport());

        h.server.script("healthy", (i, r) -> DownstreamResponse.ok("good-data"));
        h.server.script("broken", (i, r) -> DownstreamResponse.ok("ok"));
        h.server.script("slow", (i, r) -> {
            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return DownstreamResponse.ok("late");
        });

        long start = System.nanoTime();
        AggregatedResponse response = orchestrator.aggregate(threeParts(), Duration.ofMillis(300));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsedMillis < 900,
                "慢下游不得拖垮整体，编排须在整体时限附近返回: " + elapsedMillis + "ms");
        PartResult slow = response.parts().get(2);
        assertFalse(slow.present());
        assertEquals(FailureCategory.TIMEOUT, slow.category(),
                "整体时限到期的分片按 TIMEOUT 降级并标明原因");
        assertEquals("slow-default", slow.value());
        assertTrue(response.parts().get(0).present(), "快速完成的健康分片必须保留");
        log.info("整体时限300ms 实际返回={}ms slow分片类别=TIMEOUT降级 依据={}",
                elapsedMillis, slow.reason());
    }

    @Test
    void degradedWriteMarksAmbiguousSideEffectConsistently() {
        TestFixtures.Harness h = harness();
        h.withPolicy(TestFixtures.policy("writer", Duration.ofMillis(100), 5, 10,
                Duration.ofHours(1), 1, OperationKind.NON_IDEMPOTENT_WRITE));
        Orchestrator orchestrator = new Orchestrator(h.client, h.server.transport());

        h.server.script("writer", (i, r) -> {
            try {
                Thread.sleep(400);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return DownstreamResponse.confirmedWrite("written");
        });

        PartSpec spec = new PartSpec("writer", new DownstreamRequest(null, "charge"),
                () -> "write-uncertain");
        AggregatedResponse response = orchestrator.aggregate(List.of(spec), Duration.ofSeconds(1));

        PartResult part = response.parts().get(0);
        assertFalse(part.present());
        assertEquals(FailureCategory.TIMEOUT, part.category());
        assertEquals(1, part.attempts(), "非幂等写超时不得重试");
        assertEquals("write-uncertain", part.value());
        // 降级路径与重试路径对同一副作用的判定必须一致
        assertTrue(h.rules.isSideEffectAmbiguous(OperationKind.NON_IDEMPOTENT_WRITE,
                FailureCategory.TIMEOUT, false));
        log.info("写降级: 类别=TIMEOUT 尝试=1 降级值=write-uncertain 判定=副作用状态未知（与禁重试一致） 依据={}",
                part.reason());
    }
}
