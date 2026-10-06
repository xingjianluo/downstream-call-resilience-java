package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

import com.github.xingjianluo.downstreamcallresiliencejava.resilience.PolicyProvider;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResiliencePolicy;

import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 按下游名称隔离的回退缓存注册表：每个下游一个 {@link FallbackCache}，
 * 容量上限与有效期取自该下游自己的 {@link ResiliencePolicy}，
 * 一个下游的回退数据不与其他下游混淆、也不互相淘汰。
 */
public final class FallbackCacheRegistry {

    private final PolicyProvider policyProvider;
    private final Clock clock;
    private final ConcurrentMap<String, FallbackCache> caches = new ConcurrentHashMap<>();

    public FallbackCacheRegistry(PolicyProvider policyProvider, Clock clock) {
        this.policyProvider = policyProvider;
        this.clock = clock;
    }

    public FallbackCache get(String downstreamName) {
        return caches.computeIfAbsent(downstreamName, name -> {
            ResiliencePolicy policy = policyProvider.policyFor(name);
            return new FallbackCache(name, policy.fallbackCapacity(), policy.fallbackTtl(), clock);
        });
    }

    /**
     * 清空指定下游的回退数据（演示重置用）。
     */
    public void clear(String downstreamName) {
        FallbackCache cache = caches.get(downstreamName);
        if (cache != null) {
            cache.clear();
        }
    }

    /**
     * 当前已知的全部下游缓存（只读状态查询用）。
     */
    public java.util.Collection<FallbackCache> all() {
        return caches.values();
    }
}
