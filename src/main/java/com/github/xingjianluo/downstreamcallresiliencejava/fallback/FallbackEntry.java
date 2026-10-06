package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

/**
 * 回退缓存中的一条最近成功结果。
 *
 * @param key        该下游内区分请求的键（由请求派生）
 * @param value      成功响应体
 * @param capturedAt 成功结果被缓存的时间（epoch millis）
 */
public record FallbackEntry(String key, String value, long capturedAt) {
}
