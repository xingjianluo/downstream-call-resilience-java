package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.CachedFallback;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamTransport;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CallOutcome;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureRules;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResilienceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 多下游调用编排。
 * <ul>
 *   <li>各下游并发调用；即使某个下游不可用，整体响应也在约定时限内返回；</li>
 *   <li>读操作失败时优先回退该下游最近一次成功的结果（标注旧数据与新鲜度），
 *       无可用旧数据或操作不适合回退时才用静态缺省值；写操作绝不回退旧数据，
 *       该判定与重试判定共用 {@link FailureRules} 同一份规则；</li>
 *   <li>每个分片标注取值来源（{@link PartSource}）与原因，缺失分片语义不变；</li>
 *   <li>整体时限到期时仍未结束的分片记为降级（类别 TIMEOUT），不拖垮整体响应。</li>
 * </ul>
 */
public class Orchestrator {

    private static final Logger log = LoggerFactory.getLogger(Orchestrator.class);

    private final ResilienceClient client;
    private final DownstreamTransport transport;
    private final FallbackCacheRegistry fallbackCaches; // 为 null 表示未启用旧数据回退
    private final FailureRules rules;
    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("orchestration-", 0).factory());

    /**
     * 兼容旧用法：不启用旧数据回退，降级行为与之前完全一致（静态缺省值）。
     */
    public Orchestrator(ResilienceClient client, DownstreamTransport transport) {
        this(client, transport, null, new FailureRules());
    }

    public Orchestrator(ResilienceClient client, DownstreamTransport transport,
                        FallbackCacheRegistry fallbackCaches, FailureRules rules) {
        this.client = client;
        this.transport = transport;
        this.fallbackCaches = fallbackCaches;
        this.rules = rules;
    }

    public AggregatedResponse aggregate(List<PartSpec> parts, Duration overallDeadline) {
        long start = System.nanoTime();
        long deadlineNanos = start + overallDeadline.toNanos();

        List<CompletableFuture<PartResult>> futures = new ArrayList<>(parts.size());
        for (PartSpec spec : parts) {
            futures.add(CompletableFuture.supplyAsync(() -> callOne(spec), executor));
        }

        List<PartResult> results = new ArrayList<>(parts.size());
        for (int i = 0; i < futures.size(); i++) {
            PartSpec spec = parts.get(i);
            long remaining = deadlineNanos - System.nanoTime();
            results.add(collect(spec, futures.get(i), remaining));
        }

        List<String> missing = new ArrayList<>();
        boolean degraded = false;
        for (PartResult result : results) {
            if (!result.present()) {
                missing.add(result.downstreamName() + "(" + result.category() + ":" + result.reason() + ")");
            }
            if (result.degraded()) {
                degraded = true;
            }
        }

        long elapsedMillis = Duration.ofNanos(System.nanoTime() - start).toMillis();
        AggregatedResponse response = new AggregatedResponse(missing.isEmpty(), degraded,
                List.copyOf(results), List.copyOf(missing), elapsedMillis);
        log.info("编排完成 complete={} degraded={} elapsedMs={} missing={}",
                response.complete(), response.degraded(), elapsedMillis, response.missing());
        return response;
    }

    private PartResult callOne(PartSpec spec) {
        String name = spec.downstreamName();
        DownstreamRequest request = spec.request();
        CallOutcome<DownstreamResponse> outcome = client.execute(name,
                () -> transport.invoke(name, request));
        if (outcome.success()) {
            cacheSuccessIfAllowed(spec, outcome.value().body());
            log.info("[{}] 编排分片结果=完整 来源=实时 类别=SUCCESS 尝试次数={}", name, outcome.attempts());
            return new PartResult(name, true, false, FailureCategory.SUCCESS,
                    outcome.attempts(), outcome.value().body(), "OK",
                    PartSource.LIVE, null);
        }
        return degrade(spec, outcome);
    }

    /**
     * 成功结果写入回退缓存：仅当启用回退且该操作允许旧数据回退（只读）时写入；
     * 失败结果绝不进入缓存。
     */
    private void cacheSuccessIfAllowed(PartSpec spec, String body) {
        if (fallbackCaches == null) {
            return;
        }
        OperationKind kind = client.policyOf(spec.downstreamName()).operationKind();
        if (rules.isStaleFallbackAllowed(kind)) {
            fallbackCaches.get(spec.downstreamName()).put(spec.request(), body);
        }
    }

    private PartResult degrade(PartSpec spec, CallOutcome<DownstreamResponse> outcome) {
        return fallbackPart(spec, outcome.category(), outcome.attempts(), outcome.reason());
    }

    /**
     * 统一的降级出口：读操作优先回退旧数据（标注新鲜度），否则静态缺省值。
     * 写操作（尤其结果不确定的非幂等写）绝不回退旧数据——与重试判定同一份规则。
     */
    private PartResult fallbackPart(PartSpec spec, FailureCategory category, int attempts, String reason) {
        String name = spec.downstreamName();
        if (fallbackCaches != null) {
            OperationKind kind = client.policyOf(name).operationKind();
            if (rules.isStaleFallbackAllowed(kind)) {
                CachedFallback cached = fallbackCaches.get(name).get(spec.request());
                if (cached != null) {
                    String detail = reason + "；回退到 " + cached.ageMillis() + "ms 前的最近一次成功结果";
                    log.info("[{}] 编排分片结果=降级 来源=旧数据回退 类别={} 尝试次数={} 数据年龄={}ms 判定依据={}",
                            name, category, attempts, cached.ageMillis(), detail);
                    return new PartResult(name, false, true, category, attempts,
                            cached.value(), detail, PartSource.STALE_FALLBACK, cached.ageMillis());
                }
                log.info("[{}] 旧数据回退不可用（从未成功过或已过期），改用静态兜底", name);
            } else {
                log.info("[{}] 操作类型={} 不适合回退旧数据（与禁重试判定一致），改用静态兜底", name, kind);
            }
        }
        String fallbackValue = null;
        if (spec.fallback() != null) {
            try {
                fallbackValue = spec.fallback().get();
            } catch (RuntimeException e) {
                log.warn("[{}] 降级供应自身失败: {}", name, e.toString());
            }
        }
        log.info("[{}] 编排分片结果=降级 来源=静态兜底 类别={} 尝试次数={} 判定依据={} 降级值={}",
                name, category, attempts, reason, fallbackValue);
        return new PartResult(name, false, true, category, attempts,
                fallbackValue, reason, PartSource.STATIC_FALLBACK, null);
    }

    private PartResult collect(PartSpec spec, CompletableFuture<PartResult> future, long remainingNanos) {
        if (remainingNanos <= 0) {
            future.cancel(true);
            return timedOutPart(spec);
        }
        try {
            return future.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            return timedOutPart(spec);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return fallbackPart(spec, FailureCategory.REJECTED, 0, "编排等待被中断");
        } catch (ExecutionException e) {
            return fallbackPart(spec, FailureCategory.DOWNSTREAM_ERROR, 0,
                    "分片执行异常: " + e.getCause());
        }
    }

    private PartResult timedOutPart(PartSpec spec) {
        log.info("[{}] 编排分片结果=降级 类别=TIMEOUT 依据=超过整体时限，未等待该分片",
                spec.downstreamName());
        return fallbackPart(spec, FailureCategory.TIMEOUT, 0, "超过整体时限，按降级返回部分结果");
    }
}
