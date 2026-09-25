package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

/**
 * 按下游名称提供策略。
 */
@FunctionalInterface
public interface PolicyProvider {

    ResiliencePolicy policyFor(String downstreamName);
}
