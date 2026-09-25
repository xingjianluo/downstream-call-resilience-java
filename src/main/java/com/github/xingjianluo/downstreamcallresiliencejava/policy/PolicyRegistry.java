package com.github.xingjianluo.downstreamcallresiliencejava.policy;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 按下游名称保存策略；未配置时回退到默认策略。
 * <p>
 * 每个下游的超时 / 重试 / 熔断参数互不影响：A 下游的故障永远不会改变 B 下游的熔断状态。
 */
public class PolicyRegistry {

    private final Map<String, DownstreamPolicy> policies = new LinkedHashMap<>();
    private volatile DownstreamPolicy defaultPolicy = new DownstreamPolicy();

    public PolicyRegistry() {
    }

    public PolicyRegistry(Map<String, DownstreamPolicy> policies) {
        policies.values().forEach(this::register);
    }

    public final void register(DownstreamPolicy policy) {
        if (policy.getName() == null || policy.getName().isBlank()) {
            throw new IllegalArgumentException("policy.name must not be blank");
        }
        synchronized (policies) {
            policies.put(policy.getName(), policy);
        }
    }

    public final DownstreamPolicy get(String downstream) {
        synchronized (policies) {
            DownstreamPolicy p = policies.get(downstream);
            if (p != null) {
                return p;
            }
        }
        // 未配置的下游：复制一份默认策略并以该下游名称登记，保证各自独立
        synchronized (policies) {
            return policies.computeIfAbsent(downstream, name -> {
                DownstreamPolicy p = copy(defaultPolicy);
                p.setName(name);
                return p;
            });
        }
    }

    public final void setDefaultPolicy(DownstreamPolicy defaultPolicy) {
        this.defaultPolicy = defaultPolicy;
    }

    private static DownstreamPolicy copy(DownstreamPolicy s) {
        DownstreamPolicy p = new DownstreamPolicy();
        p.setTimeoutMs(s.getTimeoutMs());
        p.setReadMaxAttempts(s.getReadMaxAttempts());
        p.setWriteMaxAttempts(s.getWriteMaxAttempts());
        p.setFailureThreshold(s.getFailureThreshold());
        p.setOpenStateMs(s.getOpenStateMs());
        p.setHalfOpenPermits(s.getHalfOpenPermits());
        p.setHalfOpenSuccessThreshold(s.getHalfOpenSuccessThreshold());
        p.setRetryBackoffMs(s.getRetryBackoffMs());
        return p;
    }
}
