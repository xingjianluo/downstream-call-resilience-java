package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

/**
 * 对下游的一次实际调用（由传输层实现），可能抛出
 * {@link com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamException}。
 */
@FunctionalInterface
public interface DownstreamCall<T> {

    T invoke() throws Exception;
}
