package com.github.xingjianluo.downstreamcallresiliencejava.mock;

/**
 * 模拟下游传输层：按名称区分不同下游，各自独立统计与脚本编排。
 */
@FunctionalInterface
public interface DownstreamTransport {

    DownstreamResponse invoke(String downstreamName, DownstreamRequest request);
}
