package com.github.xingjianluo.downstreamcallresiliencejava.idempotency;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 副作用台账：写操作“是否已被下游确认生效”的唯一事实来源。
 * <p>
 * 调用方在以下时机使用同一份台账，保证判定口径一致：
 * <ul>
 *   <li>重试前：若该幂等键已确认生效，则直接复用结果，绝不再次执行写操作；</li>
 *   <li>降级时：依据台账标注该部分副作用是否生效（即使最终失败类别为 RESPONSE_LOST）。</li>
 * </ul>
 * 线程安全：基于 {@link ConcurrentHashMap}，插入为 putIfAbsent 语义，
 * 确认为 CAS 语义（用 replace(key, old, new)）。
 */
public class SideEffectLedger {

    private final ConcurrentHashMap<String, SideEffectRecord> records = new ConcurrentHashMap<>();

    public SideEffectLedger() {
    }

    /** 复合键：下游名称 + 幂等键，避免不同下游之间串用 */
    public static String compositeKey(String downstream, String idempotencyKey) {
        if (idempotencyKey == null) {
            throw new IllegalArgumentException("idempotencyKey required for ledger lookup");
        }
        return downstream + "#" + idempotencyKey;
    }

    public Optional<SideEffectRecord> find(String key) {
        return Optional.ofNullable(records.get(key));
    }

    /**
     * 仅当不存在时插入。
     *
     * @return 若已存在则返回已有记录，否则返回新插入的记录
     */
    public SideEffectRecord recordIfAbsent(String key, SideEffectRecord record) {
        SideEffectRecord existing = records.putIfAbsent(key, record);
        return existing != null ? existing : record;
    }

    /**
     * 把一条“语义不明”的记录更新为“已确认生效”。
     *
     * @return 更新成功返回新记录；已被其他线程确认则返回当前记录（携带的值可能不同，以已存在者为准）
     */
    public SideEffectRecord confirm(String key, Object value) {
        while (true) {
            SideEffectRecord current = records.get(key);
            if (current == null) {
                SideEffectRecord fresh = SideEffectRecord.confirmed(key, value, 0);
                SideEffectRecord existing = records.putIfAbsent(key, fresh);
                if (existing == null) {
                    return fresh;
                }
                current = existing;
            }
            if (current.confirmed()) {
                return current;
            }
            SideEffectRecord confirmed = SideEffectRecord.confirmed(key, value, current.deliveryAttempt());
            if (records.replace(key, current, confirmed)) {
                return confirmed;
            }
        }
    }

    public int size() {
        return records.size();
    }
}
