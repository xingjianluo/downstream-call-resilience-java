package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

/**
 * 熔断器状态：关闭（正常放行）/ 打开（快速失败）/ 半开（有限探测）。
 */
public enum CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
}
