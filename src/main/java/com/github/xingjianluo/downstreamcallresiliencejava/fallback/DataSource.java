package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

/**
 * 编排分片数据来源：调用方可据此区分实时数据、旧数据回退与静态兜底。
 */
public enum DataSource {
    /** 本次调用实时从下游取得的成功结果。 */
    LIVE,
    /** 读操作失败后回退的该下游最近一次成功结果（旧数据）。 */
    STALE,
    /** 无可用旧数据时使用的静态缺省值（原有降级语义）。 */
    STATIC_DEFAULT
}
