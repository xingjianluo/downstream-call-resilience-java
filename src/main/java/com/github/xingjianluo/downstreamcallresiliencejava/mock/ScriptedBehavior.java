package com.github.xingjianluo.downstreamcallresiliencejava.mock;

/**
 * 可编排的下游脚本行为：本地复现延迟、故障与重复响应。
 */
public interface ScriptedBehavior {

    /**
     * 第 n 次调用（从 0 开始）的行为。
     */
    DownstreamResponse behave(int invocationIndex, DownstreamRequest request);
}
