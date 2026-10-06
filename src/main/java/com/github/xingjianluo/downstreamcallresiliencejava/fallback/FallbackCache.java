package com.github.xingjianluo.downstreamcallresiliencejava.fallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 单个下游的"最近一次成功结果"回退缓存。
 *
 * <h3>淘汰规则（明确、可解释）</h3>
 * <ol>
 *   <li><b>过期优先</b>：条目年龄超过 {@code ttl} 立即失效，绝不再作为有效旧数据回退；
 *       过期条目在写入/查找时被惰性清理（队头最旧，遇第一个未过期即停）。</li>
 *   <li><b>容量 LRU</b>：清理过期后若条数仍达到 {@code capacity}，
 *       按"最近最少访问"（访问顺序，命中刷新）淘汰最旧的一条。</li>
 * </ol>
 *
 * <h3>复杂度与并发</h3>
 * <ul>
 *   <li>查找、写入、过期清理、LRU 淘汰均为摊还 O(1)：{@link LinkedHashMap} 访问序
 *       保证 LRU 淘汰取到最旧条目；插入队列只在头部累积、队头消费，每个键摊还 O(1)。</li>
 *   <li>所有变更在同一监视器上串行化，"内容（map）与计数（size）永远一致"，
 *       多线程并发写入不会超额、不会丢计数。</li>
 *   <li>缓存操作纯内存、无 I/O，回退路径不会再对下游发起调用。</li>
 * </ul>
 */
public final class FallbackCache {

    private static final Logger log = LoggerFactory.getLogger(FallbackCache.class);

    private final String name;
    private final int capacity;
    private final long ttlMillis;
    private final Clock clock;

    // accessOrder=true：get 命中即移到最近使用端，迭代器首元素恒为 LRU 最旧条目
    private final LinkedHashMap<String, FallbackEntry> entries;
    // 插入标记队列：队头恒为最老的插入候选，供 O(1) 过期清理。标记不可变并携带捕获时间：
    // 同键被覆盖刷新或被 LRU 淘汰后，旧标记与当前条目不匹配，清理时直接丢弃。
    private final Deque<InsertMarker> insertionOrder = new ArrayDeque<>();

    public FallbackCache(String downstreamName, int capacity, Duration ttl, Clock clock) {
        this.name = downstreamName;
        this.capacity = Math.max(1, capacity);
        this.ttlMillis = Math.max(0, ttl == null ? 0 : ttl.toMillis());
        this.clock = clock;
        this.entries = new LinkedHashMap<>(this.capacity + 1, 0.75f, true);
    }

    public String name() {
        return name;
    }

    public int capacity() {
        return capacity;
    }

    public long ttlMillis() {
        return ttlMillis;
    }

    /**
     * 缓存一条成功结果（仅成功结果可进入；覆盖同键旧值并刷新其新鲜度）。
     */
    public synchronized void store(String key, String value) {
        purgeExpired();
        FallbackEntry existing = entries.get(key);
        if (existing == null && entries.size() >= capacity) {
            evictLru();
        }
        FallbackEntry fresh = new FallbackEntry(key, value, clock.millis());
        entries.put(key, fresh);
        insertionOrder.addLast(new InsertMarker(key, fresh.capturedAt()));
        log.debug("[{}] 回退缓存写入 key={} 有效条数={}/{} ttl={}ms",
                name, key, entries.size(), capacity, ttlMillis);
    }

    /**
     * 查找仍在有效期内的最近成功结果；不存在或已过期返回 null（过期顺带清理）。
     * 命中会刷新 LRU 访问顺序。
     */
    public synchronized FallbackEntry findValid(String key) {
        purgeExpired();
        FallbackEntry entry = entries.get(key);
        if (entry == null) {
            return null;
        }
        long age = clock.millis() - entry.capturedAt();
        if (age > ttlMillis) {
            // ttl 边界：age <= ttl 仍有效，> ttl 才过期
            entries.remove(key);
            log.debug("[{}] 回退缓存命中但已过期 key={} age={}ms ttl={}ms", name, key, age, ttlMillis);
            return null;
        }
        log.debug("[{}] 回退缓存命中 key={} age={}ms ttl={}ms", name, key, age, ttlMillis);
        return entry;
    }

    /**
     * 当前有效条数（顺带清理过期）。与 map 内容在同一监视器内读取，保证一致。
     */
    public synchronized int size() {
        purgeExpired();
        return entries.size();
    }

    public synchronized void clear() {
        entries.clear();
        insertionOrder.clear();
    }

    /**
     * 最旧有效条目的年龄（ms）；无条目时返回 -1。仅供只读状态查询。
     */
    public synchronized long oldestEntryAgeMillis() {
        purgeExpired();
        long oldestAt = Long.MAX_VALUE;
        for (FallbackEntry entry : entries.values()) {
            if (entry.capturedAt() < oldestAt) {
                oldestAt = entry.capturedAt();
            }
        }
        return oldestAt == Long.MAX_VALUE ? -1 : clock.millis() - oldestAt;
    }

    /**
     * 从插入队头开始清理过期条目；遇到第一个仍有效的即停止（后面的插入只会更新）。
     * 标记与当前条目不匹配（键已被覆盖刷新或已被 LRU 淘汰）时丢弃陈旧标记。摊还 O(1)。
     */
    private void purgeExpired() {
        long now = clock.millis();
        while (true) {
            InsertMarker marker = insertionOrder.peekFirst();
            if (marker == null) {
                return;
            }
            FallbackEntry entry = entries.get(marker.key());
            if (entry == null || entry.capturedAt() != marker.capturedAt()) {
                insertionOrder.pollFirst();
                continue;
            }
            if (now - entry.capturedAt() > ttlMillis) {
                insertionOrder.pollFirst();
                entries.remove(marker.key());
                log.debug("[{}] 回退缓存过期淘汰 key={} age={}ms ttl={}ms",
                        name, marker.key(), now - entry.capturedAt(), ttlMillis);
                continue;
            }
            return;
        }
    }

    /**
     * LRU 淘汰：访问序 map 的迭代器首元素即最近最少访问条目。O(1)。
     */
    private void evictLru() {
        Iterator<Map.Entry<String, FallbackEntry>> it = entries.entrySet().iterator();
        if (it.hasNext()) {
            Map.Entry<String, FallbackEntry> eldest = it.next();
            it.remove();
            log.info("[{}] 回退缓存容量达 {}，按 LRU 淘汰 key={}", name, capacity, eldest.getKey());
        }
    }

    /**
     * 插入标记：不可变，携带捕获时间，用于识别已被覆盖/淘汰的陈旧队列标记。
     */
    private record InsertMarker(String key, long capturedAt) {
    }
}
