package com.github.xingjianluo.downstreamcallresiliencejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 策略配置（application.properties 前缀 resilience.*）。
 */
@ConfigurationProperties(prefix = "resilience")
public class ResilienceProperties {

    private Map<String, DownstreamProps> downstreams = new LinkedHashMap<>();

    public Map<String, DownstreamProps> getDownstreams() {
        return downstreams;
    }

    public void setDownstreams(Map<String, DownstreamProps> downstreams) {
        this.downstreams = downstreams;
    }

    public static class DownstreamProps {
        private long timeoutMillis = 1000;
        private int maxAttempts = 3;
        private long retryBackoffMillis = 0;
        private int failureThreshold = 5;
        private long openStateMillis = 10_000;
        private int halfOpenPermits = 1;
        private String operationKind = "READ";

        public long getTimeoutMillis() { return timeoutMillis; }
        public void setTimeoutMillis(long v) { this.timeoutMillis = v; }
        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int v) { this.maxAttempts = v; }
        public long getRetryBackoffMillis() { return retryBackoffMillis; }
        public void setRetryBackoffMillis(long v) { this.retryBackoffMillis = v; }
        public int getFailureThreshold() { return failureThreshold; }
        public void setFailureThreshold(int v) { this.failureThreshold = v; }
        public long getOpenStateMillis() { return openStateMillis; }
        public void setOpenStateMillis(long v) { this.openStateMillis = v; }
        public int getHalfOpenPermits() { return halfOpenPermits; }
        public void setHalfOpenPermits(int v) { this.halfOpenPermits = v; }
        public String getOperationKind() { return operationKind; }
        public void setOperationKind(String v) { this.operationKind = v; }
    }
}
