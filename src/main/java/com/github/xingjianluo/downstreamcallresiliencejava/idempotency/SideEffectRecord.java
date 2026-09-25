package com.github.xingjianluo.downstreamcallresiliencejava.idempotency;

import com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind;

/**
 * 一次写操作副作用的判定记录。重试路径与降级路径都必须读取同一条记录，保证判定一致。
 */
public record SideEffectRecord(
        String key,
        boolean confirmed,
        Object value,
        FailureKind failureKind,
        long createdAtMs,
        int deliveryAttempt) {

    public static SideEffectRecord confirmed(String key, Object value, int attempt) {
        return new SideEffectRecord(key, true, value, FailureKind.SUCCESS, System.currentTimeMillis(), attempt);
    }

    public static SideEffectRecord uncertain(String key, FailureKind kind, int attempt) {
        return new SideEffectRecord(key, false, null, kind, System.currentTimeMillis(), attempt);
    }
}
