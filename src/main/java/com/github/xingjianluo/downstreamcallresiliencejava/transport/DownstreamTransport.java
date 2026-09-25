package com.github.xingjianluo.downstreamcallresiliencejava.transport;

import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamRequest;

/**
 * 下游传输层抽象。真实环境可替换为 HTTP / RPC 客户端；测试中由本地模拟器实现。
 */
public interface DownstreamTransport {

    /**
     * 执行一次底层调用（单次尝试，不含超时/重试/熔断）。
     */
    TransportOutcome invoke(DownstreamRequest request) throws InterruptedException;
}
