package com.github.xingjianluo.downstreamcallresiliencejava.config;

import com.github.xingjianluo.downstreamcallresiliencejava.caller.DownstreamCaller;
import com.github.xingjianluo.downstreamcallresiliencejava.idempotency.SideEffectLedger;
import com.github.xingjianluo.downstreamcallresiliencejava.orchestrator.Orchestrator;
import com.github.xingjianluo.downstreamcallresiliencejava.policy.DownstreamPolicy;
import com.github.xingjianluo.downstreamcallresiliencejava.policy.PolicyRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.DownstreamSpec;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.ScriptStep;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.SimulatedDownstreamTransport;
import com.github.xingjianluo.downstreamcallresiliencejava.sim.StepKind;
import com.github.xingjianluo.downstreamcallresiliencejava.transport.DownstreamTransport;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * 弹性能力装配：策略注册表、副作用台账、模拟下游、调用器、编排器。
 * 全部基于本地模拟器，无需任何真实外部依赖即可复现各类失败场景。
 */
@Configuration
@EnableConfigurationProperties(ResilienceProperties.class)
public class ResilienceAutoConfiguration {

    @Bean
    public PolicyRegistry policyRegistry(ResilienceProperties props) {
        PolicyRegistry registry = new PolicyRegistry();
        props.getPolicies().forEach((name, p) -> registry.register(toPolicy(name, p)));
        return registry;
    }

    @Bean
    public SideEffectLedger sideEffectLedger() {
        return new SideEffectLedger();
    }

    @Bean
    public ScheduledExecutorService resilienceScheduler() {
        return new ScheduledThreadPoolExecutor(2);
    }

    @Bean
    public SimulatedDownstreamTransport downstreamTransport(ResilienceProperties props) {
        Map<String, DownstreamSpec> specs = new LinkedHashMap<>();
        props.getSim().getDownstreams().forEach((name, script) -> {
            List<ScriptStep> steps = script.getSteps().stream()
                    .map(ResilienceAutoConfiguration::toStep)
                    .toList();
            specs.put(name, new DownstreamSpec(name, script.isIdempotencyAware(), steps));
        });
        return new SimulatedDownstreamTransport(specs);
    }

    @Bean
    public DownstreamCaller downstreamCaller(DownstreamTransport transport,
                                             PolicyRegistry policyRegistry,
                                             SideEffectLedger ledger,
                                             ScheduledExecutorService resilienceScheduler) {
        return new DownstreamCaller(transport, policyRegistry, ledger, resilienceScheduler);
    }

    @Bean
    public ExecutorService orchestrationExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    public Orchestrator orchestrator(DownstreamCaller caller, ExecutorService orchestrationExecutor) {
        return new Orchestrator(caller, orchestrationExecutor);
    }

    private static DownstreamPolicy toPolicy(String name, ResilienceProperties.PolicyProps p) {
        DownstreamPolicy d = new DownstreamPolicy();
        d.setName(name);
        if (p.getTimeoutMs() != null) d.setTimeoutMs(p.getTimeoutMs());
        if (p.getReadMaxAttempts() != null) d.setReadMaxAttempts(p.getReadMaxAttempts());
        if (p.getWriteMaxAttempts() != null) d.setWriteMaxAttempts(p.getWriteMaxAttempts());
        if (p.getFailureThreshold() != null) d.setFailureThreshold(p.getFailureThreshold());
        if (p.getOpenStateMs() != null) d.setOpenStateMs(p.getOpenStateMs());
        if (p.getHalfOpenPermits() != null) d.setHalfOpenPermits(p.getHalfOpenPermits());
        if (p.getHalfOpenSuccessThreshold() != null) d.setHalfOpenSuccessThreshold(p.getHalfOpenSuccessThreshold());
        if (p.getRetryBackoffMs() != null) d.setRetryBackoffMs(p.getRetryBackoffMs());
        return d;
    }

    private static ScriptStep toStep(ScriptProperties.StepProps s) {
        StepKind kind = StepKind.valueOf(s.getKind().trim().toUpperCase());
        return new ScriptStep(kind, s.getDelayMs(), s.getValue(),
                s.getErrorCode() != null ? s.getErrorCode() : defaultErrorCode(kind));
    }

    private static String defaultErrorCode(StepKind kind) {
        return switch (kind) {
            case CONNECT_FAILURE -> "CONN_REFUSED";
            case ERROR_RESPONSE -> "DOWNSTREAM_500";
            case RESPONSE_LOST -> "RESPONSE_LOST";
            default -> null;
        };
    }
}
