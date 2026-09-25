package com.github.xingjianluo.downstreamcallresiliencejava.orchestrator;

import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamRequest;

import java.util.List;

/**
 * 一次编排请求：多个下游调用 + 整体约定时限。
 */
public record OrchestrationRequest(
        String requestId,
        List<DownstreamRequest> calls,
        long deadlineMs) {
}
