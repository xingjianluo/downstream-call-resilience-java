package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

import java.util.List;

/**
 * 编排聚合响应：包含各分片结果、是否完整、缺失部分及整体判定依据。
 */
public record AggregatedResponse(
        boolean complete,
        boolean degraded,
        List<PartResult> parts,
        List<String> missing,
        long elapsedMillis
) {
}
