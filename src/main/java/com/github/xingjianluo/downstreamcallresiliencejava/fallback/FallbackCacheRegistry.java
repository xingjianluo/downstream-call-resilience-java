package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * 按下游名称隔离的回退缓存注册表：每个下游独立的容量与有效期，
 * 一个下游的回退数据不影响其他下游。
 */
public class FallbackCacheRegistry {

    private final Function<String, FallbackPolicy> policyProvider;
    private final Clock clock;
    private final ConcurrentMap<String, FallbackCache> caches = new ConcurrentHashMap<>();

    public FallbackCacheRegistry(Function<String, FallbackPolicy> policyProvider, Clock clock) {
        this.policyProvider = policyProvider;
        this.clock = clock;
    }

    public FallbackCache get(String downstreamName) {
        return caches.computeIfAbsent(downstreamName,
                name -> new FallbackCache(name, policyProvider.apply(name), clock));
    }

    /**
     * 各下游回退缓存占用快照（只读，供查询入口；不修改任何缓存内容，
     * 过期清理由 {@link FallbackCache#stats()} 自身完成）。
     */
    public Map<String, FallbackCacheStats> stats() {
        Map<String, FallbackCacheStats> result = new LinkedHashMap<>();
        caches.forEach((name, cache) -> result.put(name, cache.stats()));
        return result;
    }
}
