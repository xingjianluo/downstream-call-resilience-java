package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;

import java.util.function.Supplier;

/**
 * 编排中的一个下游分片：名称、请求以及失败时的降级值供应。
 */
public record PartSpec(
        String downstreamName,
        DownstreamRequest request,
        Supplier<String> fallback
) {
}
