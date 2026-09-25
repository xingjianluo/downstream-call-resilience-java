package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

/**
 * 可区分的下游失败类别。
 */
public enum FailureCategory {
    SUCCESS,
    TIMEOUT,
    CONNECTION_FAILED,
    DOWNSTREAM_ERROR,
    CIRCUIT_OPEN,
    REJECTED
}
