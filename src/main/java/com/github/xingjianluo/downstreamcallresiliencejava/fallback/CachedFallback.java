package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

/**
 * 命中回退缓存的旧数据及其新鲜度。
 *
 * @param value           最近一次成功的响应体
 * @param ageMillis       距成功写入的时长（数据大概是多久之前的）
 * @param storedAtMillis  成功写入时刻（时钟毫秒）
 */
public record CachedFallback(String value, long ageMillis, long storedAtMillis) {
}
