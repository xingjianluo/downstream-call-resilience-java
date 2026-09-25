package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

/**
 * 一次调用结束后对"是否重试"的统一判定，重试路径与降级路径共用，保证判定一致。
 *
 * @param retry     是否允许重试
 * @param reason    判定依据（可解释，打印到测试日志）
 */
public record RetryDecision(boolean retry, String reason) {

    public static RetryDecision no(String reason) {
        return new RetryDecision(false, reason);
    }

    public static RetryDecision yes(String reason) {
        return new RetryDecision(true, reason);
    }
}
