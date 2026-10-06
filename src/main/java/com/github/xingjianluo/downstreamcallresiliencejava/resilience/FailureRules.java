package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

/**
 * 失败判定规则：集中定义"哪些失败可重试""失败如何归类"，
 * 重试路径与降级路径共用同一份判定，保证对同一副作用结论一致。
 */
public final class FailureRules {

    public FailureRules() {
    }

    /**
     * 判定一次失败的尝试是否可以重试。
     * <p>判定依据（可解释）：
     * <ul>
     *   <li>成功 / 熔断快速失败 / 容量拒绝：不重试；</li>
     *   <li>写操作已被下游确认生效：绝不重试（避免重复执行）；</li>
     *   <li>达到最大尝试次数：不重试；</li>
     *   <li>连接失败：请求未到达下游，任何操作都可安全重试；</li>
     *   <li>超时 / 下游错误：只读与幂等写可重试（幂等写靠幂等键由下游去重），
     *       非幂等写副作用状态未知，禁止重试。</li>
     * </ul>
     */
    public RetryDecision decideRetry(OperationKind kind, FailureCategory category,
                                    boolean sideEffectConfirmed, int attempt, int maxAttempts) {
        if (category == FailureCategory.SUCCESS) {
            return RetryDecision.no("调用成功，无需重试");
        }
        if (category == FailureCategory.CIRCUIT_OPEN) {
            return RetryDecision.no("熔断打开或半开探测名额已满，快速失败，不再重试");
        }
        if (category == FailureCategory.REJECTED) {
            return RetryDecision.no("请求被拒绝（无可用容量），不再重试");
        }
        if (sideEffectConfirmed) {
            return RetryDecision.no("写操作已被下游确认生效，禁止重复执行，不再重试");
        }
        if (attempt >= maxAttempts) {
            return RetryDecision.no("已达最大尝试次数 " + maxAttempts + "，停止重试");
        }
        return switch (category) {
            case CONNECTION_FAILED ->
                    RetryDecision.yes("连接失败，请求未到达下游，重试安全");
            case TIMEOUT -> {
                if (kind == OperationKind.NON_IDEMPOTENT_WRITE) {
                    yield RetryDecision.no("非幂等写发生超时，副作用是否生效未知，禁止重试");
                }
                yield RetryDecision.yes(kind == OperationKind.IDEMPOTENT_WRITE
                        ? "幂等写超时，凭幂等键重试安全（下游负责去重）"
                        : "只读请求超时，重试安全");
            }
            case DOWNSTREAM_ERROR -> {
                if (kind == OperationKind.NON_IDEMPOTENT_WRITE) {
                    yield RetryDecision.no("非幂等写收到下游错误响应，副作用是否生效未知，禁止重试");
                }
                yield RetryDecision.yes(kind == OperationKind.IDEMPOTENT_WRITE
                        ? "幂等写收到下游错误，凭幂等键重试安全（下游负责去重）"
                        : "只读请求收到下游错误响应，重试安全");
            }
            default -> RetryDecision.no("未覆盖的失败类别 " + category + "，保守不重试");
        };
    }

    /**
     * 旧数据回退是否允许：仅只读操作允许拿"最近一次成功结果"当降级返回。
     * <p>写操作（无论是否幂等）绝不允许把旧结果当成功返回——尤其是
     * {@link OperationKind#NON_IDEMPOTENT_WRITE} 结果不确定时，
     * 与 {@link #decideRetry} 的禁止重试、{@link #isSideEffectAmbiguous} 的
     * 副作用未知判定保持一致：同一份规则，不会出现"一边判定不能重发、
     * 另一边却把旧结果当成功返回"的矛盾。
     */
    public boolean isStaleFallbackAllowed(OperationKind kind) {
        return kind == OperationKind.READ;
    }

    /**
     * 降级路径使用同一判定：该失败是否意味着副作用状态未知。
     * <p>仅当"非幂等写 + 超时/下游错误 + 未被确认"时副作用未知；
     * 与 {@link #decideRetry} 的禁止重试结论保持一致。
     */
    public boolean isSideEffectAmbiguous(OperationKind kind, FailureCategory category,
                                         boolean sideEffectConfirmed) {
        if (sideEffectConfirmed) {
            return false;
        }
        if (kind != OperationKind.NON_IDEMPOTENT_WRITE) {
            return false;
        }
        return category == FailureCategory.TIMEOUT || category == FailureCategory.DOWNSTREAM_ERROR;
    }
}
