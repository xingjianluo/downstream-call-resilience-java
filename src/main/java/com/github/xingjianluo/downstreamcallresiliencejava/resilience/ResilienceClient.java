package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.xingjianluo.downstreamcallresiliencejava.mock.DownstreamException;

/**
 * 面向单个下游调用的弹性执行器：整合超时、重试与熔断。
 * <ul>
 *   <li>每次尝试在线程中执行，按下游各自的超时时间限时等待；超时归为 {@link FailureCategory#TIMEOUT}；</li>
 *   <li>连接失败归为 {@link FailureCategory#CONNECTION_FAILED}，下游错误归为 {@link FailureCategory#DOWNSTREAM_ERROR}；</li>
 *   <li>熔断打开 / 半开名额满时立即返回 {@link FailureCategory#CIRCUIT_OPEN}，快速失败、不等待；</li>
 *   <li>是否重试完全由 {@link FailureRules} 决定，重试路径与降级路径共用同一份副作用判定；</li>
 *   <li>每个下游每次尝试的结果与判定依据都打印到日志。</li>
 * </ul>
 */
public class ResilienceClient {

    private static final Logger log = LoggerFactory.getLogger(ResilienceClient.class);

    private final CircuitBreakerRegistry registry;
    private final FailureRules rules;
    private final ExecutorService executor;

    public ResilienceClient(CircuitBreakerRegistry registry, FailureRules rules) {
        this(registry, rules,
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("downstream-call-", 0).factory()));
    }

    ResilienceClient(CircuitBreakerRegistry registry, FailureRules rules, ExecutorService executor) {
        this.registry = registry;
        this.rules = rules;
        this.executor = executor;
    }

    /**
     * 熔断器注册表（只读状态查询复用，不改变调用语义）。
     */
    public CircuitBreakerRegistry registry() {
        return registry;
    }

    public <T> CallOutcome<T> execute(String downstreamName, DownstreamCall<T> call) {
        CircuitBreaker breaker = registry.get(downstreamName);
        ResiliencePolicy policy = breaker.policy();
        int attempt = 0;

        while (true) {
            attempt++;

            CircuitBreaker.Permit permit = breaker.tryAcquire();
            if (permit == null) {
                log.info("[{}] attempt={} 判定=快速失败 类别=CIRCUIT_OPEN 依据={} 状态={}",
                        downstreamName, attempt, "熔断打开或半开探测名额已满", breaker.snapshot());
                return CallOutcome.failure(downstreamName, FailureCategory.CIRCUIT_OPEN,
                        attempt - 1, "熔断打开，请求未发出而快速失败");
            }

            AttemptResult<T> result = runAttempt(downstreamName, attempt, policy, call);

            if (result.success()) {
                breaker.recordSuccess(permit);
                log.info("[{}] attempt={} 判定=成功 熔断记账=成功 状态={}",
                        downstreamName, attempt, breaker.snapshot());
                return CallOutcome.success(downstreamName, attempt, result.value());
            }

            breaker.recordFailure(permit, result.category());
            RetryDecision decision = rules.decideRetry(policy.operationKind(), result.category(),
                    result.sideEffectConfirmed(), attempt, policy.maxAttempts());
            log.info("[{}] attempt={} 类别={} 熔断记账=失败 判定={} 依据={} 状态={}",
                    downstreamName, attempt, result.category(),
                    decision.retry() ? "重试" : "终止", decision.reason(), breaker.snapshot());

            if (!decision.retry()) {
                String reason = decision.reason() + "；" + result.detail();
                return CallOutcome.failure(downstreamName, result.category(), attempt, reason);
            }

            if (!policy.retryBackoff().isZero()) {
                try {
                    Thread.sleep(policy.retryBackoff().toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CallOutcome.failure(downstreamName, result.category(), attempt,
                            "重试等待被中断；" + result.detail());
                }
            }
        }
    }

    private <T> AttemptResult<T> runAttempt(String downstreamName, int attempt,
                                            ResiliencePolicy policy, DownstreamCall<T> call) {
        Future<T> future = executor.submit((Callable<T>) call::invoke);
        try {
            T value = future.get(policy.timeout().toMillis(), TimeUnit.MILLISECONDS);
            return new AttemptResult<>(true, value, FailureCategory.SUCCESS, false, "成功响应");
        } catch (TimeoutException e) {
            future.cancel(true);
            return new AttemptResult<>(false, null, FailureCategory.TIMEOUT, false,
                    "超过下游超时 " + policy.timeout().toMillis() + "ms 未响应");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DownstreamException de) {
                return new AttemptResult<>(false, null, de.category(), de.sideEffectConfirmed(),
                        de.getMessage());
            }
            return new AttemptResult<>(false, null, FailureCategory.DOWNSTREAM_ERROR, false,
                    "下游抛出未预期异常: " + cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new AttemptResult<>(false, null, FailureCategory.REJECTED, false,
                    "调用线程被中断");
        }
    }

    private record AttemptResult<T>(boolean success, T value, FailureCategory category,
                                    boolean sideEffectConfirmed, String detail) {
    }
}
