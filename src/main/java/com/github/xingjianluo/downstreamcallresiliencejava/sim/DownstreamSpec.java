package com.github.xingjianluo.downstreamcallresiliencejava.sim;

import java.util.List;

/**
 * 一个模拟下游的定义：脚本 + 是否支持幂等键去重。
 */
public record DownstreamSpec(
        String name,
        boolean idempotencyAware,
        List<ScriptStep> script) {
}
