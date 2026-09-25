package com.github.xingjianluo.downstreamcallresiliencejava.tests.support;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitBreakerRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureRules;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResilienceClient;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResiliencePolicy;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试装配：为每个下游提供独立策略的弹性客户端与模拟服务器。
 */
public final class TestFixtures {

    private TestFixtures() {
    }

    public static ResiliencePolicy policy(String name, Duration timeout, int maxAttempts,
                                          int failureThreshold, Duration openState,
                                          int halfOpenPermits, OperationKind kind) {
        return new ResiliencePolicy(name, timeout, maxAttempts, Duration.ZERO,
                failureThreshold, openState, halfOpenPermits, kind);
    }

    public static final class Harness {
        public final MutableClock clock;
        public final MockDownstreamServer server;
        public final CircuitBreakerRegistry registry;
        public final ResilienceClient client;
        public final FailureRules rules;
        private final Map<String, ResiliencePolicy> policies = new HashMap<>();

        public Harness() {
            this(new MutableClock());
        }

        public Harness(MutableClock clock) {
            this.clock = clock;
            this.server = new MockDownstreamServer(clock);
            this.rules = new FailureRules();
            this.registry = new CircuitBreakerRegistry(
                    name -> policies.getOrDefault(name, ResiliencePolicy.defaults(name)), clock);
            this.client = new ResilienceClient(registry, rules);
        }

        public Harness withPolicy(ResiliencePolicy policy) {
            policies.put(policy.downstreamName(), policy);
            return this;
        }

        public ResiliencePolicy policyOf(String name) {
            return policies.get(name);
        }
    }

    public static Clock fixedSystemClock() {
        return Clock.systemUTC();
    }
}
