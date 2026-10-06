package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

/**
 * 单个下游回退缓存的只读占用快照（供查询入口展示）。
 *
 * @param size      当前缓存条数（与内容严格一致）
 * @param capacity  条数上限
 * @param ttlMillis 有效期
 */
public record FallbackCacheStats(int size, int capacity, long ttlMillis) {
}
