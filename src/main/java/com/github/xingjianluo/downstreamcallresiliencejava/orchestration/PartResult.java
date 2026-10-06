package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.DataSource;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;

/**
 * 单个分片的结果。
 * <p>在原有 {@code present/degraded/category/attempts/reason} 语义之上，
 * 新增数据来源 {@link DataSource} 与新鲜度，调用方可区分：
 * <ul>
 *   <li>{@link DataSource#LIVE}：实时成功数据；</li>
 *   <li>{@link DataSource#STALE}：该下游最近一次成功的旧数据回退（{@code staleAgeMillis} 为年龄）；</li>
 *   <li>{@link DataSource#STATIC_DEFAULT}：无可用旧数据时的静态缺省值（原有降级）。</li>
 * </ul>
 * 记录字段为"只增不改"：原构造参数顺序与含义保持不变，旧字段语义不破坏。
 */
public record PartResult(
        String downstreamName,
        boolean present,
        boolean degraded,
        FailureCategory category,
        int attempts,
        String value,
        String reason,
        DataSource source,
        long staleAgeMillis
) {

    /** 实时成功分片。 */
    public static PartResult live(String name, int attempts, String value) {
        return new PartResult(name, true, false, FailureCategory.SUCCESS,
                attempts, value, "OK", DataSource.LIVE, -1);
    }

    /** 旧数据回退分片：缺实时结果（present=false），但带回该下游最近一次成功值。 */
    public static PartResult stale(String name, FailureCategory category, int attempts,
                                   String value, long ageMillis, String reason) {
        return new PartResult(name, false, true, category, attempts, value, reason,
                DataSource.STALE, ageMillis);
    }

    /** 静态缺省值降级分片（原有降级语义）。 */
    public static PartResult staticDefault(String name, FailureCategory category, int attempts,
                                           String value, String reason) {
        return new PartResult(name, false, true, category, attempts, value, reason,
                DataSource.STATIC_DEFAULT, -1);
    }
}
