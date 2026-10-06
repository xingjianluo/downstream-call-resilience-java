package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;

/**
 * 单个分片的结果，标明缺失部分与原因。
 * <p>{@code source} 标明取值来源（实时 / 旧数据回退 / 静态兜底）；
 * {@code ageMillis} 仅在旧数据回退时有值，表示数据距成功写入的时长。
 */
public record PartResult(
        String downstreamName,
        boolean present,
        boolean degraded,
        FailureCategory category,
        int attempts,
        String value,
        String reason,
        PartSource source,
        Long ageMillis
) {

    /**
     * 兼容旧语义的构造器：实时结果记为 {@link PartSource#LIVE}，
     * 降级结果记为 {@link PartSource#STATIC_FALLBACK}（无旧数据回退时的行为）。
     */
    public PartResult(String downstreamName, boolean present, boolean degraded,
                      FailureCategory category, int attempts, String value, String reason) {
        this(downstreamName, present, degraded, category, attempts, value, reason,
                present ? PartSource.LIVE : PartSource.STATIC_FALLBACK, null);
    }
}
