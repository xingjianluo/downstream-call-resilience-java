package com.github.xingjianluo.downstreamcallresiliencejava.caller;

import com.github.xingjianluo.downstreamcallresiliencejava.breaker.CircuitBreaker;
import com.github.xingjianluo.downstreamcallresiliencejava.breaker.CircuitState;
import com.github.xingjianluo.downstreamcallresiliencejava.idempotency.SideEffectLedger;
import com.github.xingjianluo.downstreamcallresiliencejava.idempotency.SideEffectRecord;
import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamResult;
import com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind;
import com.github.xingjianluo.downstreamcallresiliencejava.policy.DownstreamPolicy;
import com.github.xingjianluo.downstreamcallresiliencejava.policy.PolicyRegistry;
import com.github.xingjianluo.downstreamcallresiliencejava.transport.DownstreamTransport;
import com.github.xingjianluo.downstreamcallresiliencejava.transport.TransportOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 单个下游调用入口：在传输层之上叠加 超时 -> 熔断 -> 重试 -> 副作用判定。
 *
 * <h3>失败类别</h3>
 * TIMEOUT / CONNECT_FAILURE / DOWNSTREAM_ERROR / CIRCUIT_OPEN / RESPONSE_LOST 各自可区分。
 *
 * <h3>熔断</h3>
 * 熔断器打开时直接返回 CIRCUIT_OPEN，不发起底层调用、不等待；
 * 每次逻辑调用（无论内部重试几次）只向熔断器记账一次，计数不随重试膨胀。
 *
 * <h3>重试与写副作用</h3>
 * <ul>
 *   <li>读：按 readMaxAttempts 重试可重试失败（超时 / 连接失败 / 下游错误）；</li>
 *   <li>非幂等写：永不重试，避免下游已经生效而再次执行；</li>
 *   <li>幂等写：重试复用同一 idempotencyKey，先查 {@link SideEffectLedger}，
 *       已确认生效则直接复用结果，绝不再次投递；响应丢失时标记为“语义不明”，
 *       不会盲目重试导致重复写。</li>
 * </ul>
 */
public class DownstreamCaller {

    private static final Logger log = LoggerFactory.getLogger(DownstreamCaller.class);

    private final DownstreamTransport transport;
    private final PolicyRegistry policyRegistry;
    private final SideEffectLedger ledger;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService attemptExecutor;
    private final boolean ownsScheduler;
    private final ConcurrentHashMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    public DownstreamCaller(DownstreamTransport transport,
                            PolicyRegistry policyRegistry,
                            SideEffectLedger ledger,
                            ScheduledExecutorService scheduler) {
        this.transport = transport;
        this.policyRegistry = policyRegistry;
        this.ledger = ledger;
        this.scheduler = scheduler;
        this.ownsScheduler = false;
        this.attemptExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public DownstreamResult call(DownstreamRequest request) {
        return call(request, Long.MAX_VALUE);
    }

    public DownstreamResult call(DownstreamRequest request, long deadlineNanoTime) {
        long startNs = System.nanoTime();
        String name = request.getDownstream();
        DownstreamPolicy policy = policyRegistry.get(name);
        CircuitBreaker breaker = circuitBreaker(name);

        // 幂等写：优先查台账，已确认生效则直接复用，绝不再次执行
        String ledgerKey = null;
        if (request.isWrite() && request.getIdempotencyKey() != null) {
            ledgerKey = SideEffectLedger.compositeKey(name, request.getIdempotencyKey());
            Optional<SideEffectRecord> existing = ledger.find(ledgerKey);
            if (existing.isPresent() && existing.get().confirmed()) {
                SideEffectRecord rec = existing.get();
                long elapsed = elapsedMs(startNs);
                log.info("[{}] result=SUCCESS(ledger-hit) attempts=0 elapsedMs={} basis=已由台账确认写副作用生效，直接复用结果，不再发起调用",
                        name, elapsed);
                return DownstreamResult.success(name, rec.value(), 0, elapsed, false, true, true,
                        breaker.currentState());
            }
        }

        // 熔断许可：拿不到则快速失败，不等待下游
        if (!breaker.tryAcquire()) {
            long elapsed = elapsedMs(startNs);
            log.info("[{}] result=FAIL kind=CIRCUIT_OPEN attempts=0 elapsedMs={} basis=熔断器为{}/探测位已满，快速失败",
                    name, elapsed, breaker.currentState());
            return DownstreamResult.failure(name, FailureKind.CIRCUIT_OPEN, "CIRCUIT_OPEN", 0, elapsed,
                    false, breaker.currentState());
        }

        int maxAttempts = request.isWrite()
                ? Math.max(1, policy.getWriteMaxAttempts())
                : Math.max(1, policy.getReadMaxAttempts());
        // 非幂等写没有去重保障，最多只允许投递一次
        if (request.isWrite() && request.getIdempotencyKey() == null) {
            maxAttempts = 1;
        }

        int attempts = 0;
        FailureKind lastKind = null;
        String lastCode = null;
        boolean sideEffectConfirmed = false;
        boolean duplicate = false;
        boolean responseLost = false;

        try {
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                attempts = attempt;
                long remainingMs = remaining(deadlineNanoTime);
                if (remainingMs <= 0) {
                    lastKind = FailureKind.TIMEOUT;
                    lastCode = "DEADLINE_EXHAUSTED";
                    breaker.releasePermit();
                    log.info("[{}] result=FAIL kind=TIMEOUT attempt={} basis=整体时限已耗尽，放弃本次尝试并归还探测许可",
                            name, attempt);
                    break;
                }

                // 幂等写每次尝试前再次查台账：可能被前序并发请求确认
                if (ledgerKey != null) {
                    Optional<SideEffectRecord> rec = ledger.find(ledgerKey);
                    if (rec.isPresent() && rec.get().confirmed()) {
                        sideEffectConfirmed = true;
                        long elapsed = elapsedMs(startNs);
                        log.info("[{}] result=SUCCESS(ledger-hit) attempts={} elapsedMs={} basis=尝试前发现台账已确认生效，跳过重复写",
                                name, attempt, elapsed);
                        return DownstreamResult.success(name, rec.get().value(), attempt, elapsed,
                                false, true, true, breaker.currentState());
                    }
                }

                // 幂等写首次投递前在台账登记“语义不明”占位（重试与降级共用同一判定来源）
                if (ledgerKey != null && attempt == 1) {
                    ledger.recordIfAbsent(ledgerKey,
                            SideEffectRecord.uncertain(ledgerKey, FailureKind.TIMEOUT, attempt));
                }

                long attemptTimeout = Math.min(policy.getTimeoutMs(), remainingMs);
                AttemptResult ar = runAttempt(request, attempt, attemptTimeout, breaker);
                attempts = attempt;

                if (ar.kind == FailureKind.SUCCESS) {
                    if (ar.duplicate) {
                        duplicate = true;
                        log.info("[{}] result=SUCCESS attempt={} elapsedMs={} basis=下游重复投递响应，已按同一调用去重为一次成功",
                                name, attempt, elapsedMs(startNs));
                    } else {
                        log.info("[{}] result=SUCCESS attempt={} elapsedMs={} basis=下游在超时内返回正常响应",
                                name, attempt, elapsedMs(startNs));
                    }
                    if (ledgerKey != null) {
                        SideEffectRecord rec = ledger.confirm(ledgerKey, ar.value);
                        sideEffectConfirmed = rec.confirmed();
                    } else if (request.isWrite()) {
                        sideEffectConfirmed = true;
                    }
                    breaker.recordSuccess();
                    long elapsed = elapsedMs(startNs);
                    return DownstreamResult.success(name, ar.value, attempt, elapsed, duplicate, false,
                            sideEffectConfirmed, breaker.currentState());
                }

                lastKind = ar.kind;
                lastCode = ar.code;
                if (ar.kind == FailureKind.RESPONSE_LOST) {
                    responseLost = true;
                    // 响应丢失：下游写可能已生效，记录语义不明，且不再盲目重试
                    if (ledgerKey != null) {
                        ledger.recordIfAbsent(ledgerKey,
                                SideEffectRecord.uncertain(ledgerKey, FailureKind.RESPONSE_LOST, attempt));
                    }
                    log.info("[{}] result=FAIL kind=RESPONSE_LOST attempt={} basis=写操作响应丢失，副作用可能已生效，标记语义不明且不再重试",
                            name, attempt);
                    break;
                }

                boolean retryable = isRetryable(ar.kind);
                boolean canRetry = attempt < maxAttempts;
                if (retryable && canRetry) {
                    long backoff = Math.min(policy.getRetryBackoffMs(),
                            Math.max(0, remaining(deadlineNanoTime) - policy.getTimeoutMs()));
                    log.info("[{}] result=FAIL kind={} code={} attempt={} basis=可重试失败，退避{}ms 后重试",
                            name, ar.kind, ar.code, attempt, backoff);
                    if (backoff > 0) {
                        if (!sleepBackoff(backoff, deadlineNanoTime)) {
                            lastKind = FailureKind.TIMEOUT;
                            lastCode = "DEADLINE_EXHAUSTED";
                            break;
                        }
                    }
                    continue;
                }
                log.info("[{}] result=FAIL kind={} code={} attempt={} basis={}",
                        name, ar.kind, ar.code, attempt,
                        canRetry ? "该失败类别不可重试" : "已达最大尝试次数");
                break;
            }
        } finally {
            // 若在未产出成功/失败结论前异常退出，确保许可不泄漏
        }

        long elapsed = elapsedMs(startNs);
        boolean confirmed = sideEffectConfirmed
                || (ledgerKey != null && ledger.find(ledgerKey).map(SideEffectRecord::confirmed).orElse(false));
        DownstreamResult result = DownstreamResult.failure(name, lastKind, lastCode, attempts, elapsed,
                confirmed, breaker.currentState());
        breaker.recordFailure();

        String basis = responseLost
                ? "最终失败：下游响应丢失（写副作用可能已生效），不重试"
                : "最终失败：重试已用尽或该类别不可重试，熔断计一次失败";
        log.info("[{}] final=FAIL kind={} code={} attempts={} elapsedMs={} sideEffectConfirmed={} circuit={} basis={}",
                name, lastKind, lastCode, attempts, elapsed, confirmed, breaker.currentState(), basis);
        return result;
    }

    public CircuitBreaker circuitBreaker(String downstream) {
        DownstreamPolicy p = policyRegistry.get(downstream);
        return breakers.computeIfAbsent(downstream, n -> new CircuitBreaker(
                n, p.getFailureThreshold(), p.getOpenStateMs(), p.getHalfOpenPermits(),
                p.getHalfOpenSuccessThreshold()));
    }

    private AttemptResult runAttempt(DownstreamRequest request, int attempt, long timeoutMs,
                                     CircuitBreaker breaker) {
        Future<TransportOutcome> future = attemptExecutor.submit(() -> transport.invoke(request));
        boolean interrupted = false;
        try {
            TransportOutcome out = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (out.success()) {
                return AttemptResult.success(out.value(), out.duplicateWireResponse());
            }
            return AttemptResult.fail(out.failureKind(), out.errorCode());
        } catch (TimeoutException e) {
            future.cancel(true);
            return AttemptResult.fail(FailureKind.TIMEOUT, "TIMEOUT_" + timeoutMs + "ms");
        } catch (InterruptedException e) {
            future.cancel(true);
            interrupted = true;
            Thread.currentThread().interrupt();
            return AttemptResult.fail(FailureKind.CONNECT_FAILURE, "INTERRUPTED");
        } catch (ExecutionException e) {
            future.cancel(true);
            Throwable cause = e.getCause();
            if (cause instanceof InterruptedException) {
                interrupted = true;
                return AttemptResult.fail(FailureKind.CONNECT_FAILURE, "INTERRUPTED");
            }
            log.warn("[{}] attempt={} 底层调用抛出未预期异常", request.getDownstream(), attempt, cause);
            return AttemptResult.fail(FailureKind.CONNECT_FAILURE,
                    cause == null ? "EXECUTION_ERROR" : cause.getClass().getSimpleName());
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean sleepBackoff(long backoffMs, long deadlineNanoTime) {
        long remaining = remaining(deadlineNanoTime);
        long wait = Math.min(backoffMs, remaining);
        if (wait <= 0) {
            return false;
        }
        try {
            Thread.sleep(wait);
            return remaining(deadlineNanoTime) > 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static boolean isRetryable(FailureKind kind) {
        return kind == FailureKind.TIMEOUT
                || kind == FailureKind.CONNECT_FAILURE
                || kind == FailureKind.DOWNSTREAM_ERROR;
    }

    private static long remaining(long deadlineNanoTime) {
        if (deadlineNanoTime == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        long ns = deadlineNanoTime - System.nanoTime();
        return ns <= 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(ns);
    }

    private static long elapsedMs(long startNanoTime) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanoTime);
    }

    private record AttemptResult(FailureKind kind, String code, Object value, boolean duplicate) {
        static AttemptResult success(Object value, boolean duplicate) {
            return new AttemptResult(FailureKind.SUCCESS, null, value, duplicate);
        }

        static AttemptResult fail(FailureKind kind, String code) {
            return new AttemptResult(kind, code, null, false);
        }
    }
}
