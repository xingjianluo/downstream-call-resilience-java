package com.github.xingjianluo.downstreamcallresiliencejava.model;

/**
 * 可区分的失败类别。
 */
public enum FailureKind {
    /** 调用成功 */
    SUCCESS,
    /** 等待下游响应超过时限 */
    TIMEOUT,
    /** 连接未能建立，请求未到达下游 */
    CONNECT_FAILURE,
    /** 下游返回了错误响应（业务错误 / 5xx 等） */
    DOWNSTREAM_ERROR,
    /** 熔断器打开，请求被快速失败 */
    CIRCUIT_OPEN,
    /** 响应丢失：下游可能已生效，但响应未返回（写操作语义不明） */
    RESPONSE_LOST;

    public boolean isFailure() {
        return this != SUCCESS;
    }
}
