package com.github.xingjianluo.downstreamcallresiliencejava.status;

import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitState;

/**
 * 单个下游的只读运行状态快照：健康/熔断状态 + 回退缓存占用（骨架，下一步填充）。
 */
public record DownstreamStatus(
        String downstreamName,
        CircuitState circuitState,
        int consecutiveFailures,
        long circuitRecoverInMillis,
        String operationKind,
        int fallbackSize,
        int fallbackCapacity,
        long fallbackTtlMillis,
        long oldestEntryAgeMillis
) {
}
