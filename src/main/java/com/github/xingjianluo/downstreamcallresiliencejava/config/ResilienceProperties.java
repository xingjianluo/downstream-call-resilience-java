package com.github.xingjianluo.downstreamcallresiliencejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 弹性能力配置。
 * <pre>
 * resilience.orchestration-deadline-ms=800
 * resilience.policies.<下游名>.timeout-ms=...
 * resilience.sim.downstreams.<下游名>.steps[0].kind=...
 * </pre>
 */
@ConfigurationProperties(prefix = "resilience")
public class ResilienceProperties {

    private Map<String, PolicyProps> policies = new LinkedHashMap<>();
    private long orchestrationDeadlineMs = 800;
    private SimProps sim = new SimProps();

    public Map<String, PolicyProps> getPolicies() { return policies; }
    public void setPolicies(Map<String, PolicyProps> policies) { this.policies = policies; }
    public long getOrchestrationDeadlineMs() { return orchestrationDeadlineMs; }
    public void setOrchestrationDeadlineMs(long orchestrationDeadlineMs) { this.orchestrationDeadlineMs = orchestrationDeadlineMs; }
    public SimProps getSim() { return sim; }
    public void setSim(SimProps sim) { this.sim = sim; }

    public static class SimProps {
        private Map<String, ScriptProperties> downstreams = new LinkedHashMap<>();

        public Map<String, ScriptProperties> getDownstreams() { return downstreams; }
        public void setDownstreams(Map<String, ScriptProperties> downstreams) { this.downstreams = downstreams; }
    }

    public static class PolicyProps {
        private Long timeoutMs;
        private Integer readMaxAttempts;
        private Integer writeMaxAttempts;
        private Integer failureThreshold;
        private Long openStateMs;
        private Integer halfOpenPermits;
        private Integer halfOpenSuccessThreshold;
        private Long retryBackoffMs;

        public Long getTimeoutMs() { return timeoutMs; }
        public void setTimeoutMs(Long timeoutMs) { this.timeoutMs = timeoutMs; }
        public Integer getReadMaxAttempts() { return readMaxAttempts; }
        public void setReadMaxAttempts(Integer readMaxAttempts) { this.readMaxAttempts = readMaxAttempts; }
        public Integer getWriteMaxAttempts() { return writeMaxAttempts; }
        public void setWriteMaxAttempts(Integer writeMaxAttempts) { this.writeMaxAttempts = writeMaxAttempts; }
        public Integer getFailureThreshold() { return failureThreshold; }
        public void setFailureThreshold(Integer failureThreshold) { this.failureThreshold = failureThreshold; }
        public Long getOpenStateMs() { return openStateMs; }
        public void setOpenStateMs(Long openStateMs) { this.openStateMs = openStateMs; }
        public Integer getHalfOpenPermits() { return halfOpenPermits; }
        public void setHalfOpenPermits(Integer halfOpenPermits) { this.halfOpenPermits = halfOpenPermits; }
        public Integer getHalfOpenSuccessThreshold() { return halfOpenSuccessThreshold; }
        public void setHalfOpenSuccessThreshold(Integer halfOpenSuccessThreshold) { this.halfOpenSuccessThreshold = halfOpenSuccessThreshold; }
        public Long getRetryBackoffMs() { return retryBackoffMs; }
        public void setRetryBackoffMs(Long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }
    }
}
