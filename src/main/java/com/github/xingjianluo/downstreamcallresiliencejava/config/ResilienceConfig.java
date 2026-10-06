package com.github.xingjianluo.downstreamcallresiliencejava.config;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamTransport;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.MockDownstreamServer;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestration.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.status.ResilienceStatusService;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitBreakerRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureRules;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.PolicyProvider;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResilienceClient;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResiliencePolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;

/**
 * Spring Bean 装配。
 */
@Configuration
@EnableConfigurationProperties(ResilienceProperties.class)
public class ResilienceConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public MockDownstreamServer mockDownstreamServer(Clock clock) {
        return new MockDownstreamServer(clock);
    }

    @Bean
    public DownstreamTransport downstreamTransport(MockDownstreamServer server) {
        return server.transport();
    }

    @Bean
    public PolicyProvider policyProvider(ResilienceProperties props) {
        return name -> {
            ResilienceProperties.DownstreamProps p = props.getDownstreams().get(name);
            if (p == null) {
                return ResiliencePolicy.defaults(name);
            }
            return new ResiliencePolicy(
                    name,
                    Duration.ofMillis(p.getTimeoutMillis()),
                    Math.max(1, p.getMaxAttempts()),
                    Duration.ofMillis(p.getRetryBackoffMillis()),
                    Math.max(1, p.getFailureThreshold()),
                    Duration.ofMillis(p.getOpenStateMillis()),
                    Math.max(1, p.getHalfOpenPermits()),
                    OperationKind.valueOf(p.getOperationKind()),
                    Math.max(1, p.getFallbackCapacity()),
                    Duration.ofMillis(Math.max(0, p.getFallbackTtlMillis())));
        };
    }

    @Bean
    public FailureRules failureRules() {
        return new FailureRules();
    }

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(PolicyProvider provider, Clock clock) {
        return new CircuitBreakerRegistry(provider, clock);
    }

    @Bean
    public ResilienceClient resilienceClient(CircuitBreakerRegistry registry, FailureRules rules) {
        return new ResilienceClient(registry, rules);
    }

    @Bean
    public FallbackCacheRegistry fallbackCacheRegistry(PolicyProvider provider, Clock clock) {
        return new FallbackCacheRegistry(provider, clock);
    }

    @Bean
    public Orchestrator orchestrator(ResilienceClient client, DownstreamTransport transport,
                                     CircuitBreakerRegistry breakerRegistry,
                                     FallbackCacheRegistry fallbackRegistry,
                                     PolicyProvider policyProvider, Clock clock) {
        return new Orchestrator(client, transport, breakerRegistry,
                fallbackRegistry, policyProvider, clock);
    }

    @Bean
    public ResilienceStatusService resilienceStatusService(CircuitBreakerRegistry breakerRegistry,
                                                           FallbackCacheRegistry fallbackRegistry,
                                                           Clock clock) {
        return new ResilienceStatusService(breakerRegistry, fallbackRegistry, clock);
    }
}
