package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

import java.time.Duration;

/**
 * 单个下游的回退缓存策略（不可变值对象）。
 *
 * @param capacity 条数上限：超过即按明确规则（最久未使用）淘汰
 * @param ttl      有效期：超过有效期的旧数据不得再当有效数据回退
 */
public record FallbackPolicy(int capacity, Duration ttl) {

    public static FallbackPolicy defaults() {
        return new FallbackPolicy(64, Duration.ofMinutes(1));
    }
}
