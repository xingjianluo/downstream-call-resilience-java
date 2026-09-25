package com.github.xingjianluo.downstreamcallresiliencejava.policy;

/**
 * 单个下游的弹性策略：超时 / 重试 / 熔断。按下游分别配置、分别生效。
 */
public class DownstreamPolicy {

    private String name;
    /** 单次尝试的超时（毫秒） */
    private long timeoutMs = 500;
    /** 读操作最大尝试次数（含首次） */
    private int readMaxAttempts = 2;
    /** 幂等写最大尝试次数（含首次）；非幂等写永不重试 */
    private int writeMaxAttempts = 2;
    /** 连续失败多少次后熔断打开 */
    private int failureThreshold = 3;
    /** 打开后多久进入半开（毫秒） */
    private long openStateMs = 1_000;
    /** 半开状态下允许同时参与探测的请求数 */
    private int halfOpenPermits = 1;
    /** 半开期间连续成功多少次后关闭熔断 */
    private int halfOpenSuccessThreshold = 2;
    /** 两次尝试之间的退避（毫秒） */
    private long retryBackoffMs = 20;

    public DownstreamPolicy() {
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public long getTimeoutMs() { return timeoutMs; }
    public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
    public int getReadMaxAttempts() { return readMaxAttempts; }
    public void setReadMaxAttempts(int readMaxAttempts) { this.readMaxAttempts = readMaxAttempts; }
    public int getWriteMaxAttempts() { return writeMaxAttempts; }
    public void setWriteMaxAttempts(int writeMaxAttempts) { this.writeMaxAttempts = writeMaxAttempts; }
    public int getFailureThreshold() { return failureThreshold; }
    public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }
    public long getOpenStateMs() { return openStateMs; }
    public void setOpenStateMs(long openStateMs) { this.openStateMs = openStateMs; }
    public int getHalfOpenPermits() { return halfOpenPermits; }
    public void setHalfOpenPermits(int halfOpenPermits) { this.halfOpenPermits = halfOpenPermits; }
    public int getHalfOpenSuccessThreshold() { return halfOpenSuccessThreshold; }
    public void setHalfOpenSuccessThreshold(int halfOpenSuccessThreshold) { this.halfOpenSuccessThreshold = halfOpenSuccessThreshold; }
    public long getRetryBackoffMs() { return retryBackoffMs; }
    public void setRetryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }
}
