package com.github.xingjianluo.downstreamcallresiliencejava.transport;

import com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind;

/**
 * 传输层单次尝试的原始结果。
 *
 * @param duplicateWireResponse 同一尝试内下游重复投递了响应（模拟重复响应）
 * @param serverExecuted        下游服务端是否实际执行了副作用（用于测试侧验证）
 */
public record TransportOutcome(
        boolean success,
        Object value,
        FailureKind failureKind,
        String errorCode,
        long latencyMs,
        boolean duplicateWireResponse,
        boolean serverExecuted) {

    public static TransportOutcome ok(Object value, long latencyMs, boolean duplicate, boolean serverExecuted) {
        return new TransportOutcome(true, value, FailureKind.SUCCESS, null, latencyMs, duplicate, serverExecuted);
    }

    public static TransportOutcome fail(FailureKind kind, String errorCode, long latencyMs, boolean serverExecuted) {
        return new TransportOutcome(false, null, kind, errorCode, latencyMs, false, serverExecuted);
    }
}
