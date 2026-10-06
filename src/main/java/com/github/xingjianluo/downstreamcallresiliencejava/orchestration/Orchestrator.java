package com.github.xingjianluo.downstreamcallresiliencejava.orchestration;

import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCache;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackCacheRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.fallback.FallbackEntry;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamResponse;
import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamTransport;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CallOutcome;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.CircuitBreakerRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.OperationKind;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.PolicyProvider;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResilienceClient;
import com.github.xingjianluo.downstreamcallresiliencejava.resilience.ResiliencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
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
 *   <li>读分片失败时，优先回退该下游同请求最近一次成功结果（标注旧数据与年龄）；
 *       从未成功过 / 旧数据已过期 / 非读操作时，才使用静态缺省值；</li>
 *   <li>写操作（尤其副作用未知时）绝不拿旧结果当成功返回，与 {@code FailureRules}
 *       的"该不该重试 / 副作用是否未知"判定保持一致；</li>
 *   <li>仅成功的读结果会进入回退缓存，失败结果绝不缓存；</li>
 *   <li>整体时限到期时仍未结束的分片记为降级（类别 TIMEOUT），不拖垮整体响应。</li>
 * </ul>
 */
public class Orchestrator {

    private static final Logger log = LoggerFactory.getLogger(Orchestrator.class);

    private final ResilienceClient client;
    private final DownstreamTransport transport;
    private final CircuitBreakerRegistry breakerRegistry;
    private final FallbackCacheRegistry fallbackRegistry;
    private final PolicyProvider policyProvider;
    private final Clock clock;
    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("orchestration-", 0).factory());

    /**
     * 兼容旧装配：无回退缓存时，内部按默认策略构建一份（行为退化为"全部静态兜底"，
     * 只要传入相同的策略提供者，后续成功结果也会被缓存）。
     */
    public Orchestrator(ResilienceClient client, DownstreamTransport transport) {
        this(client, transport, client.registry(),
                new FallbackCacheRegistry(name ->
                        client.registry().get(name).policy(), Clock.systemUTC()),
                name -> client.registry().get(name).policy(), Clock.systemUTC());
    }

    public Orchestrator(ResilienceClient client, DownstreamTransport transport,
                        CircuitBreakerRegistry breakerRegistry,
                        FallbackCacheRegistry fallbackRegistry,
                        PolicyProvider policyProvider, Clock clock) {
        this.client = client;
        this.transport = transport;
        this.breakerRegistry = breakerRegistry;
        this.fallbackRegistry = fallbackRegistry;
        this.policyProvider = policyProvider;
        this.clock = clock;
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
                missing.add(result.downstreamName() + "(" + result.category() + ":" + result.reason()
                        + ",来源=" + result.source() + ")");
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
        OperationKind kind = policyProvider.policyFor(name).operationKind();
        CallOutcome<DownstreamResponse> outcome = client.execute(name,
                () -> transport.invoke(name, request));
        if (outcome.success()) {
            // 只有成功的读结果才允许进入回退缓存：失败不缓存，写结果不缓存
            if (kind == OperationKind.READ) {
                fallbackRegistry.get(name).store(cacheKey(request), outcome.value().body());
            }
            log.info("[{}] 编排分片结果=完整 来源=LIVE(实时) 类别=SUCCESS 尝试次数={}",
                    name, outcome.attempts());
            return PartResult.live(name, outcome.attempts(), outcome.value().body());
        }
        return degrade(spec, outcome, kind);
    }

    private PartResult degrade(PartSpec spec, CallOutcome<DownstreamResponse> outcome,
                               OperationKind kind) {
        String name = spec.downstreamName();
        // 写操作（含幂等写/非幂等写）一律不回退旧结果；
        // 非幂等写副作用未知时尤其不能把旧结果当成功——与 FailureRules 禁重试结论一致。
        if (kind != OperationKind.READ) {
            String note = kind == OperationKind.NON_IDEMPOTENT_WRITE ? "（写副作用状态未知，禁止以旧结果冒充成功）" : "";
            PartResult result = PartResult.staticDefault(name, outcome.category(), outcome.attempts(),
                    staticFallback(spec), outcome.reason());
            log.info("[{}] 编排分片结果=降级缺失 来源=STATIC_DEFAULT(静态兜底) 类别={} 尝试次数={} "
                            + "判定依据=写操作不回退旧数据{} 降级值={}",
                    name, outcome.category(), outcome.attempts(), note, result.value());
            return result;
        }

        FallbackEntry stale = fallbackRegistry.get(name).findValid(cacheKey(spec.request()));
        if (stale != null) {
            long age = clock.millis() - stale.capturedAt();
            String reason = outcome.reason() + "；回退最近一次成功旧数据（age=" + age + "ms）";
            PartResult result = PartResult.stale(name, outcome.category(), outcome.attempts(),
                    stale.value(), age, reason);
            log.info("[{}] 编排分片结果=降级(旧数据可用) 来源=STALE(旧数据回退) 类别={} 尝试次数={} "
                            + "新鲜度={}ms前 判定依据={} 旧值={}",
                    name, outcome.category(), outcome.attempts(), age, outcome.reason(), stale.value());
            return result;
        }

        PartResult result = PartResult.staticDefault(name, outcome.category(), outcome.attempts(),
                staticFallback(spec), outcome.reason()
                + "；无有效旧数据（从未成功或已过期），使用静态缺省值");
        log.info("[{}] 编排分片结果=降级缺失 来源=STATIC_DEFAULT(静态兜底) 类别={} 尝试次数={} "
                        + "判定依据=无有效旧数据({}) 降级值={}",
                name, outcome.category(), outcome.attempts(), outcome.reason(), result.value());
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
            return PartResult.staticDefault(spec.downstreamName(), FailureCategory.REJECTED,
                    0, staticFallback(spec), "编排等待被中断");
        } catch (ExecutionException e) {
            return PartResult.staticDefault(spec.downstreamName(), FailureCategory.DOWNSTREAM_ERROR,
                    0, staticFallback(spec), "分片执行异常: " + e.getCause());
        }
    }

    private PartResult timedOutPart(PartSpec spec) {
        // 整体时限截断的读分片同样优先尝试旧数据回退
        String name = spec.downstreamName();
        ResiliencePolicy policy = policyProvider.policyFor(name);
        if (policy.operationKind() == OperationKind.READ) {
            FallbackEntry stale = fallbackRegistry.get(name).findValid(cacheKey(spec.request()));
            if (stale != null) {
                long age = clock.millis() - stale.capturedAt();
                log.info("[{}] 编排分片结果=降级(旧数据可用) 来源=STALE(旧数据回退) 类别=TIMEOUT "
                        + "依据=超过整体时限 新鲜度={}ms前", name, age);
                return PartResult.stale(name, FailureCategory.TIMEOUT, 0, stale.value(), age,
                        "超过整体时限，按旧数据回退返回部分结果（age=" + age + "ms）");
            }
        }
        log.info("[{}] 编排分片结果=降级缺失 来源=STATIC_DEFAULT(静态兜底) 类别=TIMEOUT "
                + "依据=超过整体时限且无有效旧数据，未等待该分片", name);
        return PartResult.staticDefault(name, FailureCategory.TIMEOUT, 0, staticFallback(spec),
                "超过整体时限，按降级返回部分结果");
    }

    private String staticFallback(PartSpec spec) {
        if (spec.fallback() == null) {
            return null;
        }
        try {
            return spec.fallback().get();
        } catch (RuntimeException e) {
            log.warn("[{}] 降级供应自身失败: {}", spec.downstreamName(), e.toString());
            return null;
        }
    }

    private static String cacheKey(DownstreamRequest request) {
        // 同一下游内按请求派生缓存键：幂等键优先，否则用请求体；null 兜底为固定键
        if (request == null) {
            return "";
        }
        if (request.idempotencyKey() != null) {
            return "idem:" + request.idempotencyKey();
        }
        return "payload:" + request.payload();
    }

    public CircuitBreakerRegistry breakerRegistry() {
        return breakerRegistry;
    }

    public FallbackCacheRegistry fallbackRegistry() {
        return fallbackRegistry;
    }

    public PolicyProvider policyProvider() {
        return policyProvider;
    }
}
