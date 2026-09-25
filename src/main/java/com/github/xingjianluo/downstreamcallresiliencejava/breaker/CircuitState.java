package com.github.xingjianluo.downstreamcallresiliencejava.breaker;

/**
 * 熔断器状态。
 */
public enum CircuitState {
    /** 关闭：请求正常通过并统计失败 */
    CLOSED,
    /** 打开：请求快速失败 */
    OPEN,
    /** 半开：仅有限数量的探测请求通过 */
    HALF_OPEN
}
