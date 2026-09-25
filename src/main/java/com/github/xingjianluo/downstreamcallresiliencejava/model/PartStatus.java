package com.github.xingjianluo.downstreamcallresiliencejava.model;

/**
 * 编排响应中单个下游部分的状态。
 */
public enum PartStatus {
    /** 该下游正常返回 */
    OK,
    /** 该下游被降级，内容缺失或使用兜底结果 */
    DEGRADED
}
