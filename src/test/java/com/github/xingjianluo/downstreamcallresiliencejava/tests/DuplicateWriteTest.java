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
 * 重复写防护：
 * 1) 幂等写首次连接失败、重试成功：副作用只执行一次（服务端幂等去重）；
 * 2) 非幂等写失败：不重试，attempts=1，避免重复执行；
 * 3) 同一调用下游重复投递响应：客户端只算一次成功；
 * 4) 响应丢失：归类 RESPONSE_LOST 且不盲目重试。
 */
class DuplicateWriteTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateWriteTest.class);

    private DownstreamCaller caller(SimulatedDownstreamTransport transport,
                                   int writeAttempts, long timeoutMs) {
        DownstreamPolicy p = new DownstreamPolicy();
        p.setName("wsvc");
        p.setTimeoutMs(timeoutMs);
        p.setReadMaxAttempts(3);
        p.setWriteMaxAttempts(writeAttempts);
        p.setFailureThreshold(10);
        p.setRetryBackoffMs(5);
        PolicyRegistry registry = new PolicyRegistry();
        registry.register(p);
        return new DownstreamCaller(transport, registry, new SideEffectLedger(),
                new ScheduledThreadPoolExecutor(1));
    }

    @Test
    void idempotentWriteRetriedButSideEffectExecutedOnce() {
        // 第 1 次投递连接失败（无副作用），第 2 次成功
        SimulatedDownstreamTransport transport = new SimulatedDownstreamTransport(Map.of(
                "wsvc", new DownstreamSpec("wsvc", true, List.of(
                        ScriptStep.connectFailure(2),
                        ScriptStep.ok("order-created")))));
        DownstreamCaller caller = caller(transport, 3, 1000);

        DownstreamResult r = caller.call(
                DownstreamRequest.write("wsvc", "order:42", "key-42"));

        log.info("[下游结果] downstream=wsvc kind={} attempts={} serverExecutions={} rawDeliveries={} basis=幂等写首次连接失败后重试，服务端副作用只生效一次",
                r.failureKind(), r.attempts(), transport.serverExecutions("wsvc"), transport.rawDeliveries("wsvc"));
        assertTrue(r.success());
        assertEquals(2, r.attempts());
        assertEquals(1, transport.serverExecutions("wsvc"), "副作用只能执行一次");
        assertEquals(2, transport.rawDeliveries("wsvc"), "底层应有两次投递（首次失败未到达）");
    }

    @Test
    void nonIdempotentWriteIsNeverRetried() {
        SimulatedDownstreamTransport transport = new SimulatedDownstreamTransport(Map.of(
                "wsvc", new DownstreamSpec("wsvc", false, List.of(ScriptStep.connectFailure(2)))));
        DownstreamCaller caller = caller(transport, 5, 1000);

        DownstreamResult r = caller.call(DownstreamRequest.write("wsvc", "order:43", null));

        log.info("[下游结果] downstream=wsvc kind={} attempts={} rawDeliveries={} basis=无幂等键的写不重试，避免下游已生效时重复执行",
                r.failureKind(), r.attempts(), transport.rawDeliveries("wsvc"));
        assertFalse(r.success());
        assertEquals(FailureKind.CONNECT_FAILURE, r.failureKind());
        assertEquals(1, r.attempts(), "非幂等写只能投递一次");
        assertEquals(1, transport.rawDeliveries("wsvc"));
    }

    @Test
    void duplicateWireResponseIsCollapsedToOneSuccess() {
        SimulatedDownstreamTransport transport = new SimulatedDownstreamTransport(Map.of(
                "wsvc", new DownstreamSpec("wsvc", true,
                        List.of(ScriptStep.duplicate("order-created", 5)))));
        DownstreamCaller caller = caller(transport, 3, 1000);

        DownstreamResult r = caller.call(
                DownstreamRequest.write("wsvc", "order:44", "key-44"));

        log.info("[下游结果] downstream=wsvc kind={} duplicateResponse={} serverExecutions={} basis=同一调用收到重复响应，去重为一次成功",
                r.failureKind(), r.duplicateResponse(), transport.serverExecutions("wsvc"));
        assertTrue(r.success());
        assertTrue(r.duplicateResponse(), "需要标记发生了重复响应");
        assertEquals(1, transport.serverExecutions("wsvc"));
    }

    @Test
    void responseLostIsClassifiedAndNotRetriedBlindly() {
        SimulatedDownstreamTransport transport = new SimulatedDownstreamTransport(Map.of(
                "wsvc", new DownstreamSpec("wsvc", true,
                        List.of(ScriptStep.responseLost(5)))));
        DownstreamCaller caller = caller(transport, 3, 1000);

        DownstreamResult r = caller.call(
                DownstreamRequest.write("wsvc", "order:45", "key-45"));

        log.info("[下游结果] downstream=wsvc kind={} attempts={} sideEffectConfirmed={} serverExecutions={} basis=响应丢失语义不明，标记并不再重试",
                r.failureKind(), r.attempts(), r.sideEffectConfirmed(), transport.serverExecutions("wsvc"));
        assertFalse(r.success());
        assertEquals(FailureKind.RESPONSE_LOST, r.failureKind());
        assertEquals(1, r.attempts(), "响应丢失不得盲目重试");
        assertEquals(1, transport.serverExecutions("wsvc"), "服务端只执行一次");
    }
}
