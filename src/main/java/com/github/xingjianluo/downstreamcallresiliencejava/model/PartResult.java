package com.github.xingjianluo.downstreamcallresiliencejava.model;

/**
 * 编排响应中单个下游的部分结果。降级时会标明缺失原因。
 */
public record PartResult(
        String downstream,
        PartStatus status,
        Object value,
        FailureKind reason,
        String reasonMessage,
        int attempts,
        long elapsedMs,
        boolean sideEffectConfirmed) {

    public static PartResult ok(DownstreamResult r) {
        return new PartResult(r.downstream(), PartStatus.OK, r.value(), null, null,
                r.attempts(), r.elapsedMs(), r.sideEffectConfirmed());
    }

    public static PartResult degraded(DownstreamResult r, String message) {
        return new PartResult(r.downstream(), PartStatus.DEGRADED, null, r.failureKind(), message,
                r.attempts(), r.elapsedMs(), r.sideEffectConfirmed());
    }
}
