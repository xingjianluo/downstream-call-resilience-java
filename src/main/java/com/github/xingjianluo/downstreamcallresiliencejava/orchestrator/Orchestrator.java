package com.github.xingjianluo.downstreamcallresiliencejava.orchestrator;

import com.github.xingjianluo.downstreamcallresiliencejava.caller.DownstreamCaller;
import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamResult;
import com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind;
import com.github.xingjianluo.downstreamcallresiliencejava.model.OrchestrationResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.model.PartResult;
import com.github.xingjianluo.downstreamcallresiliencejava.model.PartStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 多下游调用编排。
 * <ul>
 *   <li>各下游并行调用，统一受整体时限 deadlineMs 约束；</li>
 *   <li>某个下游不可用时整体响应仍在约定时限内返回，该部分标记为 DEGRADED，
 *       并在 {@link PartResult#reason()} / {@link PartResult#reasonMessage()} 中写明缺失类别与原因；</li>
 *   <li>降级部分的副作用是否生效与重试路径使用同一个 SideEffectLedger 判定，口径一致；</li>
 *   <li>全部部分 OK 时 success=true，否则 success=false（但 HTTP 层面仍返回 200，保持部分结果契约）。</li>
 * </ul>
 */
public class Orchestrator {

    private static final Logger log = LoggerFactory.getLogger(Orchestrator.class);

    private final DownstreamCaller caller;
    private final ExecutorService executor;

    public Orchestrator(DownstreamCaller caller, ExecutorService executor) {
        this.caller = caller;
        this.executor = executor;
    }

    public OrchestrationResponse orchestrate(OrchestrationRequest request) {
        long startNs = System.nanoTime();
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(request.deadlineMs());

        List<CompletableFuture<DownstreamResult>> futures = new ArrayList<>(request.calls().size());
        for (DownstreamRequest call : request.calls()) {
            CompletableFuture<DownstreamResult> f = CompletableFuture.supplyAsync(
                    () -> caller.call(call, deadlineNs), executor);
            futures.add(f);
        }

        List<PartResult> parts = new ArrayList<>(request.calls().size());
        List<String> degraded = new ArrayList<>();

        for (int i = 0; i < futures.size(); i++) {
            String downstream = request.calls().get(i).getDownstream();
            CompletableFuture<DownstreamResult> f = futures.get(i);
            DownstreamResult r;
            long remainingNs = deadlineNs - System.nanoTime();
            if (remainingNs <= 0 && !f.isDone()) {
                // 时限已到且未完成：不再等待，直接降级（caller 自身的超时/熔断保证底层不会长时间悬挂）
                r = DownstreamResult.failure(downstream, FailureKind.TIMEOUT, "ORCHESTRATION_DEADLINE",
                        0, elapsedMs(startNs), false,
                        caller.circuitBreaker(downstream).currentState());
            } else {
                try {
                    r = f.get(Math.max(0, remainingNs), TimeUnit.NANOSECONDS);
                } catch (Exception e) {
                    r = DownstreamResult.failure(downstream, FailureKind.TIMEOUT, "ORCHESTRATION_DEADLINE",
                            0, elapsedMs(startNs), false,
                            caller.circuitBreaker(downstream).currentState());
                }
            }

            if (r.success()) {
                parts.add(PartResult.ok(r));
            } else {
                degraded.add(downstream);
                parts.add(PartResult.degraded(r, explain(r)));
                log.info("[orchestrate:{}] degraded downstream={} reason={} sideEffectConfirmed={} basis=该下游不可用，按降级返回并标注缺失原因",
                        request.requestId(), downstream, r.failureKind(), r.sideEffectConfirmed());
            }
        }

        long elapsed = elapsedMs(startNs);
        boolean success = degraded.isEmpty();
        String summary = success
                ? "ALL_OK"
                : "DEGRADED missing=" + degraded + " withinDeadlineMs=" + request.deadlineMs();
        log.info("[orchestrate:{}] complete success={} elapsedMs={}/{} degraded={} basis=并行汇聚各下游结果，缺失部分已标注原因",
                request.requestId(), success, elapsed, request.deadlineMs(), degraded);
        return new OrchestrationResponse(success, elapsed, request.deadlineMs(),
                List.copyOf(parts), List.copyOf(degraded), summary);
    }

    private static String explain(DownstreamResult r) {
        return switch (r.failureKind()) {
            case TIMEOUT -> "下游在超时时间内未返回（" + r.errorCode() + "），已尝试 " + r.attempts() + " 次";
            case CONNECT_FAILURE -> "连接下游失败（" + r.errorCode() + "），请求未到达或未建立连接";
            case DOWNSTREAM_ERROR -> "下游返回错误响应（" + r.errorCode() + "）";
            case CIRCUIT_OPEN -> "该下游熔断已打开/半开探测位已满，请求被快速失败";
            case RESPONSE_LOST ->
                    "下游写响应丢失，副作用可能已生效(sideEffectConfirmed=" + r.sideEffectConfirmed() + ")，未重复执行";
            case SUCCESS -> null;
        };
    }

    private static long elapsedMs(long startNs) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
    }
}
