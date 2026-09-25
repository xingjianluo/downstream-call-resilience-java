package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;

/**
 * 单个分片的结果，标明缺失部分与原因。
 */
public record PartResult(
        String downstreamName,
        boolean present,
        boolean degraded,
        FailureCategory category,
        int attempts,
        String value,
        String reason
) {
}
