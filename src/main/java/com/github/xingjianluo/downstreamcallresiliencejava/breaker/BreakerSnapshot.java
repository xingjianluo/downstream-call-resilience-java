package com.github.xingjianluo.downstreamcallresiliencejava.breaker;

/**
 * 熔断器可观测快照。
 */
public record BreakerSnapshot(
        String name,
        CircuitState state,
        int consecutiveFailures,
        int halfOpenSuccesses,
        int halfOpenPermitsInUse,
        long openedAtMs) {
}
