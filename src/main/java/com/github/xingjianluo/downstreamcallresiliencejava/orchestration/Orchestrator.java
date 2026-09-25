package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamTransport;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CallOutcome;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;
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
 *   <li>某分片失败时执行其降级供应，{@link PartResult} 明确标注缺失分片与原因，
 *       降级判定（副作用是否未知）与重试判定共用 {@code FailureRules} 同一份规则；</li>
 *   <li>整体时限到期时仍未结束的分片记为降级（类别 TIMEOUT），不拖垮整体响应。</li>
 * </ul>
 */
public class Orchestrator {

    private static final Logger log = LoggerFactory.getLogger(Orchestrator.class);

    private final ResilienceClient client;
    private final DownstreamTransport transport;
    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("orchestration-", 0).factory());

    public Orchestrator(ResilienceClient client, DownstreamTransport transport) {
        this.client = client;
        this.transport = transport;
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
            log.info("[{}] 编排分片结果=完整 类别=SUCCESS 尝试次数={}", name, outcome.attempts());
            return new PartResult(name, true, false, FailureCategory.SUCCESS,
                    outcome.attempts(), outcome.value().body(), "OK");
        }
        return degrade(spec, outcome);
    }

    private PartResult degrade(PartSpec spec, CallOutcome<DownstreamResponse> outcome) {
        String name = spec.downstreamName();
        String fallbackValue = null;
        if (spec.fallback() != null) {
            try {
                fallbackValue = spec.fallback().get();
            } catch (RuntimeException e) {
                log.warn("[{}] 降级供应自身失败: {}", name, e.toString());
            }
        }
        PartResult result = new PartResult(name, false, true, outcome.category(),
                outcome.attempts(), fallbackValue, outcome.reason());
        log.info("[{}] 编排分片结果=降级缺失 类别={} 尝试次数={} 判定依据={} 降级值={}",
                name, outcome.category(), outcome.attempts(), outcome.reason(), fallbackValue);
        return result;
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
            return new PartResult(spec.downstreamName(), false, true, FailureCategory.REJECTED,
                    0, fallbackOf(spec), "编排等待被中断");
        } catch (ExecutionException e) {
            return new PartResult(spec.downstreamName(), false, true, FailureCategory.DOWNSTREAM_ERROR,
                    0, fallbackOf(spec), "分片执行异常: " + e.getCause());
        }
    }

    private PartResult timedOutPart(PartSpec spec) {
        log.info("[{}] 编排分片结果=降级缺失 类别=TIMEOUT 依据=超过整体时限，未等待该分片",
                spec.downstreamName());
        return new PartResult(spec.downstreamName(), false, true, FailureCategory.TIMEOUT,
                0, fallbackOf(spec), "超过整体时限，按降级返回部分结果");
    }

    private String fallbackOf(PartSpec spec) {
        return spec.fallback() == null ? null : spec.fallback().get();
    }
}
