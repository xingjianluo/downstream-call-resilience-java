package com.github.xingjianluo.downstreamcallresiliencejava.status;

import java.util.List;

/**
 * 所有已知下游的只读运行状态聚合（骨架，下一步填充）。
 */
public record ResilienceStatus(long reportedAtMillis, List<DownstreamStatus> downstreams) {
}
