package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

/**
 * 分片结果的取值来源，调用方据此区分实时数据与兜底数据。
 */
public enum PartSource {
    /** 从下游实时拿到的成功结果。 */
    LIVE,
    /** 下游失败，回退到该下游最近一次成功的旧数据（附新鲜度）。 */
    STALE_FALLBACK,
    /** 无可用旧数据或该操作不适合回退，使用静态缺省值。 */
    STATIC_FALLBACK
}
