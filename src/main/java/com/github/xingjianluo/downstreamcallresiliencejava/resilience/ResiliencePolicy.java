package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

import java.time.Duration;

/**
 * 单个下游的超时 / 重试 / 熔断 / 回退缓存策略（不可变值对象）。
 *
 * @param fallbackCapacity 每个下游最近成功结果缓存的条数上限（LRU 淘汰）
 * @param fallbackTtl      回退旧数据的有效期；超过后不再作为有效数据回退
 */
public record ResiliencePolicy(
        String downstreamName,
        Duration timeout,
        int maxAttempts,
        Duration retryBackoff,
        int failureThreshold,
        Duration openStateDuration,
        int halfOpenPermits,
        OperationKind operationKind,
        int fallbackCapacity,
        Duration fallbackTtl
) {

    /** 默认回退缓存条数上限。 */
    public static final int DEFAULT_FALLBACK_CAPACITY = 16;
    /** 默认回退旧数据有效期。 */
    public static final Duration DEFAULT_FALLBACK_TTL = Duration.ofSeconds(30);

    /**
     * 兼容旧签名（不含回退缓存配置）：使用默认容量与有效期。
     */
    public ResiliencePolicy(String downstreamName, Duration timeout, int maxAttempts,
                            Duration retryBackoff, int failureThreshold, Duration openStateDuration,
                            int halfOpenPermits, OperationKind operationKind) {
        this(downstreamName, timeout, maxAttempts, retryBackoff, failureThreshold,
                openStateDuration, halfOpenPermits, operationKind,
                DEFAULT_FALLBACK_CAPACITY, DEFAULT_FALLBACK_TTL);
    }

    public static ResiliencePolicy defaults(String name) {
        return new ResiliencePolicy(name, Duration.ofSeconds(1), 3, Duration.ZERO,
                5, Duration.ofSeconds(10), 1, OperationKind.READ,
                DEFAULT_FALLBACK_CAPACITY, DEFAULT_FALLBACK_TTL);
    }
}
