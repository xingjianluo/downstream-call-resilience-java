package com.github.xingjianluo.downstreamcallresiliencejava.mock;

/**
 * 发往下游的请求（本地模拟用）。
 *
 * @param idempotencyKey 幂等键；null 表示该写操作不具备幂等保护
 * @param payload        请求体
 */
public record DownstreamRequest(String idempotencyKey, String payload) {
}
