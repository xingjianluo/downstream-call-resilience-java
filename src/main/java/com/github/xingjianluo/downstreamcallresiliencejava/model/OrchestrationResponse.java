package com.github.xingjianluo.downstreamcallresiliencejava.model;

import java.util.List;

/**
 * 多个下游编排后的整体响应：即使部分下游不可用，也在约定时限内返回。
 */
public record OrchestrationResponse(
        boolean success,
        long elapsedMs,
        long deadlineMs,
        List<PartResult> parts,
        List<String> degradedDownstreams,
        String summary) {
}
