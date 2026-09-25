package com.github.xingjianluo.downstreamcallresiliencejava.config;

import java.util.ArrayList;
import java.util.List;

/**
 * 模拟脚本配置：resilience.sim.downstreams.<name>.*
 */
public class ScriptProperties {

    /** 该下游是否识别幂等键（服务端去重） */
    private boolean idempotencyAware = true;
    /** 有序脚本步骤 */
    private List<StepProps> steps = new ArrayList<>();

    public boolean isIdempotencyAware() { return idempotencyAware; }
    public void setIdempotencyAware(boolean idempotencyAware) { this.idempotencyAware = idempotencyAware; }
    public List<StepProps> getSteps() { return steps; }
    public void setSteps(List<StepProps> steps) { this.steps = steps; }

    public static class StepProps {
        /** OK / LATENCY / SLOW / CONNECT_FAILURE / ERROR_RESPONSE / RESPONSE_LOST / DUPLICATE_RESPONSE */
        private String kind;
        private long delayMs;
        private String value;
        private String errorCode;

        public String getKind() { return kind; }
        public void setKind(String kind) { this.kind = kind; }
        public long getDelayMs() { return delayMs; }
        public void setDelayMs(long delayMs) { this.delayMs = delayMs; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public String getErrorCode() { return errorCode; }
        public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
    }
}
