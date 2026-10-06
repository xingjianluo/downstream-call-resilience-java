package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 单个下游的回退缓存：保存该下游最近一次成功的结果，供读操作失败时回退。
 * <p>约束：
 * <ul>
 *   <li>只缓存成功结果，失败结果绝不写入（写入只发生在成功路径）；</li>
 *   <li>条数上限 + 有效期：超容量按"最久未使用（LRU）"淘汰，过期数据不得回退；</li>
 *   <li>查找与淘汰均为常数级开销：访问序 {@link LinkedHashMap} 的 get/put/淘汰均摊 O(1)；</li>
 *   <li>并发安全：所有读写在同一把锁上串行化，size 与内容始终一致，
 *       不会出现计数与内容对不上；缓存本身绝不触发下游调用。</li>
 * </ul>
 */
public class FallbackCache {

    private final String downstreamName;
    private final FallbackPolicy policy;
    private final Clock clock;
    private final long ttlMillis;

    private final Object lock = new Object();
    /** 访问序 LinkedHashMap：迭代顺序即"最久未使用在前"，淘汰表头即可。 */
    private final LinkedHashMap<DownstreamRequest, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);

    public FallbackCache(String downstreamName, FallbackPolicy policy, Clock clock) {
        this.downstreamName = downstreamName;
        this.policy = policy;
        this.clock = clock;
        this.ttlMillis = policy.ttl().toMillis();
    }

    /**
     * 写入一次成功结果（仅成功路径调用；同键覆盖并刷新新旧程度与位置）。
     */
    public void put(DownstreamRequest request, String value) {
        synchronized (lock) {
            entries.remove(request);
            entries.put(request, new Entry(value, clock.millis()));
            // 容量淘汰：LRU，最久未使用的在表头，逐条移除直到不超上限
            while (entries.size() > policy.capacity()) {
                var it = entries.entrySet().iterator();
                it.next();
                it.remove();
            }
        }
    }

    /**
     * 查找可回退的旧数据；从未成功过（未命中）或已过期返回 {@code null}。
     * 过期条目顺手移除，不再占用容量。
     */
    public CachedFallback get(DownstreamRequest request) {
        synchronized (lock) {
            Entry entry = entries.get(request);
            if (entry == null) {
                return null;
            }
            long age = clock.millis() - entry.storedAtMillis();
            if (age >= ttlMillis) {
                entries.remove(request);
                return null;
            }
            return new CachedFallback(entry.value(), age, entry.storedAtMillis());
        }
    }

    /**
     * 当前占用快照（只读）。返回前顺手清理已过期条目，使展示与可回退内容一致。
     */
    public FallbackCacheStats stats() {
        synchronized (lock) {
            long now = clock.millis();
            entries.values().removeIf(e -> now - e.storedAtMillis() >= ttlMillis);
            return new FallbackCacheStats(entries.size(), policy.capacity(), ttlMillis);
        }
    }

    public String downstreamName() {
        return downstreamName;
    }

    private record Entry(String value, long storedAtMillis) {
    }
}
