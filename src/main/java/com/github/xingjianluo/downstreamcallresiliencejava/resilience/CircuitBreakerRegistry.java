package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 按下游名称隔离的熔断器注册表：每个下游独立统计、独立熔断，
 * 一个下游熔断不会连带任何其他健康下游。
 */
public class CircuitBreakerRegistry {

    private final PolicyProvider policyProvider;
    private final Clock clock;
    private final ConcurrentMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    public CircuitBreakerRegistry(PolicyProvider policyProvider, Clock clock) {
        this.policyProvider = policyProvider;
        this.clock = clock;
    }

    public CircuitBreaker get(String downstreamName) {
        return breakers.computeIfAbsent(downstreamName,
                name -> new CircuitBreaker(name, policyProvider.policyFor(name), clock));
    }

    /**
     * 各下游熔断器状态快照（只读，供查询入口展示健康/熔断状态）。
     */
    public java.util.Map<String, String> snapshots() {
        java.util.Map<String, String> result = new java.util.LinkedHashMap<>();
        breakers.forEach((name, breaker) -> result.put(name, breaker.snapshot()));
        return result;
    }
}
