package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

import java.time.Duration;

/**
 * 单个下游的超时 / 重试 / 熔断策略（不可变值对象）。
 */
public record ResiliencePolicy(
        String downstreamName,
        Duration timeout,
        int maxAttempts,
        Duration retryBackoff,
        int failureThreshold,
        Duration openStateDuration,
        int halfOpenPermits,
        OperationKind operationKind
) {

    public static ResiliencePolicy defaults(String name) {
        return new ResiliencePolicy(name, Duration.ofSeconds(1), 3, Duration.ZERO,
                5, Duration.ofSeconds(10), 1, OperationKind.READ);
    }
}
