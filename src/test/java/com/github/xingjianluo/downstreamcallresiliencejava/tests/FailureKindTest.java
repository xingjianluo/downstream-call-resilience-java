package com.github.xingjianluo.downstreamcallresiliencejava.tests;

import com.github.xingjianluo.downstreamcallresiliencejava.caller.DownstreamCaller;
import com.github.xingjianluo.downstreamcallresiliencejava.idempotency.SideEffectLedger;
import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamResult;
import com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind;
import com.github.xingjianluo.downstreamcallresiliencejava.policy.DownstreamPolicy;
import com.github.xingjianluo.downstreamcallresiliencejava.policy.PolicyRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.DownstreamSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.ScriptStep;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.SimulatedDownstreamTransport;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 超时 / 连接失败 / 下游错误响应 必须归入可区分的失败类别，且重试次数可解释。
 */
class FailureKindTest {

    private static final Logger log = LoggerFactory.getLogger(FailureKindTest.class);

    private DownstreamCaller callerWith(String name, DownstreamPolicy policy, List<ScriptStep> steps) {
        SimulatedDownstreamTransport transport = new SimulatedDownstreamTransport(
                Map.of(name, new DownstreamSpec(name, true, steps)));
        PolicyRegistry registry = new PolicyRegistry();
        registry.register(policy);
        return new DownstreamCaller(transport, registry, new SideEffectLedger(),
                new ScheduledThreadPoolExecutor(1));
    }

    private DownstreamPolicy policy(String name, long timeoutMs, int attempts) {
        DownstreamPolicy p = new DownstreamPolicy();
        p.setName(name);
        p.setTimeoutMs(timeoutMs);
        p.setReadMaxAttempts(attempts);
        p.setWriteMaxAttempts(attempts);
        p.setFailureThreshold(10);
        p.setRetryBackoffMs(5);
        return p;
    }

    @Test
    void timeoutIsClassifiedAsTimeout() {
        DownstreamCaller caller = callerWith("slow", policy("slow", 80, 2),
                List.of(ScriptStep.latency("late", 400)));

        DownstreamResult result = caller.call(DownstreamRequest.read("slow", "q"));

        log.info("[判定依据] downstream=slow result={} kind={} attempts={} elapsedMs={} -> 400ms 延迟超过 80ms 超时，两次尝试均超时",
                result.success() ? "SUCCESS" : "FAIL", result.failureKind(), result.attempts(), result.elapsedMs());
        assertFalse(result.success());
        assertEquals(FailureKind.TIMEOUT, result.failureKind());
        assertEquals(2, result.attempts(), "读操作应按策略重试到 2 次");
    }

    @Test
    void connectFailureIsDistinctFromTimeout() {
        DownstreamCaller caller = callerWith("broken", policy("broken", 300, 2),
                List.of(ScriptStep.connectFailure(5)));

        DownstreamResult result = caller.call(DownstreamRequest.read("broken", "q"));

        log.info("[判定依据] downstream=broken result={} kind={} attempts={} -> 连接阶段失败，请求未到达，区别于 TIMEOUT",
                result.success() ? "SUCCESS" : "FAIL", result.failureKind(), result.attempts());
        assertFalse(result.success());
        assertEquals(FailureKind.CONNECT_FAILURE, result.failureKind());
    }

    @Test
    void downstreamErrorResponseIsDistinct() {
        DownstreamCaller caller = callerWith("erroring", policy("erroring", 300, 3),
                List.of(ScriptStep.error(5, "UPSTREAM_500")));

        DownstreamResult result = caller.call(DownstreamRequest.read("erroring", "q"));

        log.info("[判定依据] downstream=erroring result={} kind={} code={} attempts={} -> 下游明确返回错误响应，区别于连接失败/超时",
                result.success() ? "SUCCESS" : "FAIL", result.failureKind(), result.errorCode(), result.attempts());
        assertFalse(result.success());
        assertEquals(FailureKind.DOWNSTREAM_ERROR, result.failureKind());
        assertEquals("UPSTREAM_500", result.errorCode());
        assertEquals(3, result.attempts());
    }

    @Test
    void successPathBaseline() {
        DownstreamCaller caller = callerWith("healthy", policy("healthy", 300, 2),
                List.of(ScriptStep.ok("v1")));

        DownstreamResult result = caller.call(DownstreamRequest.read("healthy", "q"));

        log.info("[判定依据] downstream=healthy result=SUCCESS value={} attempts={} -> 首次调用即成功，无需重试",
                result.value(), result.attempts());
        assertTrue(result.success());
        assertEquals("v1", result.value());
        assertEquals(1, result.attempts());
    }
}
