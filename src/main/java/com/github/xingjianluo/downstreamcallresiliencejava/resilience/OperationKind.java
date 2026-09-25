package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

/**
 * 操作的副作用属性，决定重试与降级路径对副作用的判定。
 */
public enum OperationKind {
    /** 只读，无副作用，可安全重试。 */
    READ,
    /** 写操作，且下游支持幂等键去重，可安全重试。 */
    IDEMPOTENT_WRITE,
    /** 写操作，下游不支持幂等键，结果未知时不得重试。 */
    NON_IDEMPOTENT_WRITE
}
