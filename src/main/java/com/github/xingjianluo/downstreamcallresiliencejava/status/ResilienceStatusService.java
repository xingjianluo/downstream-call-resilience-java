package com.github.xingjianluo.downstreamcallresiliencejava.status;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCache;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitBreaker;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitBreakerRegistry;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * 只读运行状态查询：聚合每个下游的熔断/健康状态与回退缓存占用，供本地排查。
 * 不触发任何下游调用、不改变任何运行状态。
 */
public final class ResilienceStatusService {

    private final CircuitBreakerRegistry breakerRegistry;
    private final FallbackCacheRegistry fallbackRegistry;
    private final Clock clock;

    public ResilienceStatusService(CircuitBreakerRegistry breakerRegistry,
                                   FallbackCacheRegistry fallbackRegistry, Clock clock) {
        this.breakerRegistry = breakerRegistry;
        this.fallbackRegistry = fallbackRegistry;
        this.clock = clock;
    }

    public ResilienceStatus snapshot() {
        // 熔断器注册表与缓存注册表可能因初始化顺序不同而键集略有差异：取并集保证两者都能看到
        TreeSet<String> names = new TreeSet<>();
        for (CircuitBreaker breaker : breakerRegistry.all()) {
            names.add(breaker.name());
        }
        for (FallbackCache cache : fallbackRegistry.all()) {
            names.add(cache.name());
        }

        List<DownstreamStatus> list = new ArrayList<>(names.size());
        for (String name : names) {
            CircuitBreaker breaker = breakerRegistry.get(name);
            FallbackCache cache = fallbackRegistry.get(name);
            list.add(new DownstreamStatus(
                    name,
                    breaker.state(),
                    breaker.consecutiveFailures(),
                    breaker.recoverInMillis(),
                    breaker.policy().operationKind().name(),
                    cache.size(),
                    cache.capacity(),
                    cache.ttlMillis(),
                    cache.oldestEntryAgeMillis()));
        }
        return new ResilienceStatus(clock.millis(), List.copyOf(list));
    }
}
