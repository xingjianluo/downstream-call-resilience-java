package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

/**
 * 单个下游一次完整调用（可能含多次尝试）的结果。
 *
 * @param downstreamName 下游名称
 * @param success        是否成功
 * @param category       最终失败类别（成功为 SUCCESS）
 * @param attempts       实际尝试次数
 * @param value          成功时的响应体
 * @param reason         可解释的判定依据
 * @param degraded       是否走了降级
 * @param <T>            响应体类型
 */
public record CallOutcome<T>(
        String downstreamName,
        boolean success,
        FailureCategory category,
        int attempts,
        T value,
        String reason,
        boolean degraded
) {

    public static <T> CallOutcome<T> success(String name, int attempts, T value) {
        return new CallOutcome<>(name, true, FailureCategory.SUCCESS, attempts, value, "OK", false);
    }

    public static <T> CallOutcome<T> failure(String name, FailureCategory category,
                                             int attempts, String reason) {
        return new CallOutcome<>(name, false, category, attempts, null, reason, false);
    }
}
