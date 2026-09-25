package com.github.xingjianluo.downstreamcallresiliencejava.model;

import com.github.xingjianluo.downstreamcallresiliencejava.breaker.CircuitState;

/**
 * 单个下游一次完整逻辑调用（含重试）后的结果。
 */
public record DownstreamResult(
        String downstream,
        boolean success,
        Object value,
        FailureKind failureKind,
        String errorCode,
        int attempts,
        long elapsedMs,
        boolean sideEffectConfirmed,
        boolean duplicateResponse,
        boolean servedFromLedger,
        CircuitState circuitState) {

    public static DownstreamResult success(String downstream, Object value, int attempts, long elapsedMs,
                                           boolean duplicateResponse, boolean servedFromLedger,
                                           boolean sideEffectConfirmed, CircuitState state) {
        return new DownstreamResult(downstream, true, value, FailureKind.SUCCESS, null, attempts, elapsedMs,
                sideEffectConfirmed, duplicateResponse, servedFromLedger, state);
    }

    public static DownstreamResult failure(String downstream, FailureKind kind, String errorCode, int attempts,
                                           long elapsedMs, boolean sideEffectConfirmed, CircuitState state) {
        return new DownstreamResult(downstream, false, null, kind, errorCode, attempts, elapsedMs,
                sideEffectConfirmed, false, false, state);
    }
}
