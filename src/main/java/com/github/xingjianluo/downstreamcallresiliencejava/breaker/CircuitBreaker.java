package com.github.xingjianluo.downstreamcallresiliencejava.breaker;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 单个下游的熔断器（每个下游各自独立，互不影响）。
 * <p>
 * 状态机：CLOSED -> OPEN -> HALF_OPEN -> CLOSED / OPEN。
 * <ul>
 *   <li>CLOSED：连续失败达到阈值后进入 OPEN；</li>
 *   <li>OPEN：请求快速失败（不等待下游），等待 openStateMs 后允许一次 CAS 迁移到 HALF_OPEN；</li>
 *   <li>HALF_OPEN：只允许有限数量（halfOpenPermits）的探测请求通过，其余快速失败；
 *       探测失败立即回到 OPEN；连续成功达到阈值后回到 CLOSED。</li>
 * </ul>
 * 并发安全：状态为不可变对象，所有迁移走 {@link AtomicReference#compareAndSet} 自旋，
 * 计数只在 CAS 成功时生效，因此多线程并发迁移不会重复计数或丢失计数。
 */
public class CircuitBreaker {

    private final String name;
    private final int failureThreshold;
    private final long openStateMs;
    private final int halfOpenPermits;
    private final int halfOpenSuccessThreshold;
    private final AtomicReference<State> ref;

    public CircuitBreaker(String name,
                          int failureThreshold,
                          long openStateMs,
                          int halfOpenPermits,
                          int halfOpenSuccessThreshold) {
        this.name = name;
        this.failureThreshold = Math.max(1, failureThreshold);
        this.openStateMs = openStateMs;
        this.halfOpenPermits = Math.max(1, halfOpenPermits);
        this.halfOpenSuccessThreshold = Math.max(1, halfOpenSuccessThreshold);
        this.ref = new AtomicReference<>(State.closed());
    }

    public String getName() {
        return name;
    }

    /**
     * 尝试获取调用许可。
     * <p>
     * OPEN 已到冷却时间时，恰好一个线程通过 CAS 把状态迁到 HALF_OPEN，
     * 然后由 HALF_OPEN 分支占用一个探测许可；其他线程要么进入 HALF_OPEN 竞争剩余许可，
     * 要么因许可已满继续快速失败。
     *
     * @return true 允许调用下游；false 熔断打开/探测位已满，应快速失败
     */
    public boolean tryAcquire() {
        while (true) {
            State s = ref.get();
            switch (s.state) {
                case CLOSED -> {
                    return true;
                }
                case OPEN -> {
                    if (s.isCooledDown(openStateMs)) {
                        State half = State.toHalfOpen(s);
                        if (ref.compareAndSet(s, half)) {
                            // CAS 迁移成功者继续占用第一个探测许可
                            return acquireHalfOpenPermit(half);
                        }
                        // 迁移被其他线程抢先，自旋重读
                        continue;
                    }
                    return false;
                }
                case HALF_OPEN -> {
                    return acquireHalfOpenPermit(s);
                }
                default -> {
                    return false;
                }
            }
        }
    }

    private boolean acquireHalfOpenPermit(State s) {
        while (true) {
            State cur = ref.get();
            if (cur.state != CircuitState.HALF_OPEN) {
                // 状态已被其他探测结果改变，按新状态重新决策
                if (cur.state == CircuitState.CLOSED) {
                    return true;
                }
                return false;
            }
            if (cur.halfOpenPermitsInUse >= halfOpenPermits) {
                return false;
            }
            State next = cur.withPermitsInUse(cur.halfOpenPermitsInUse + 1);
            if (ref.compareAndSet(cur, next)) {
                return true;
            }
        }
    }

    /**
     * 记录一次逻辑调用成功（一次逻辑调用只记录一次，与重试次数无关）。
     */
    public void recordSuccess() {
        while (true) {
            State s = ref.get();
            switch (s.state) {
                case CLOSED -> {
                    if (s.consecutiveFailures == 0) {
                        return;
                    }
                    if (ref.compareAndSet(s, s.withConsecutiveFailures(0))) {
                        return;
                    }
                }
                case HALF_OPEN -> {
                    State released = s.withPermitsInUse(Math.max(0, s.halfOpenPermitsInUse - 1));
                    int successes = released.halfOpenSuccesses + 1;
                    State next;
                    if (successes >= halfOpenSuccessThreshold) {
                        next = State.closed();
                    } else {
                        next = released.withHalfOpenSuccesses(successes);
                    }
                    if (ref.compareAndSet(s, next)) {
                        return;
                    }
                }
                case OPEN -> {
                    // OPEN 期间理论上不会有成功结果；忽略即可
                    return;
                }
                default -> {
                    return;
                }
            }
        }
    }

    /**
     * 记录一次逻辑调用失败。
     * <p>
     * HALF_OPEN 下任何一次探测失败都会立即回到 OPEN（重置冷却计时），
     * 但由于熔断器按下游独立，健康下游的熔断器不会被连带打开。
     */
    public void recordFailure() {
        while (true) {
            State s = ref.get();
            switch (s.state) {
                case CLOSED -> {
                    int failures = s.consecutiveFailures + 1;
                    State next = (failures >= failureThreshold)
                            ? State.openNow()
                            : s.withConsecutiveFailures(failures);
                    if (ref.compareAndSet(s, next)) {
                        return;
                    }
                }
                case HALF_OPEN -> {
                    // 占用许可的探测失败：归还许可无需保留，直接重新进入 OPEN
                    if (ref.compareAndSet(s, State.openNow())) {
                        return;
                    }
                }
                case OPEN -> {
                    return;
                }
                default -> {
                    return;
                }
            }
        }
    }

    /**
     * 归还未使用的探测许可（例如拿到许可后发现没有剩余时间预算而放弃调用）。
     */
    public void releasePermit() {
        while (true) {
            State s = ref.get();
            if (s.state != CircuitState.HALF_OPEN || s.halfOpenPermitsInUse == 0) {
                return;
            }
            State next = s.withPermitsInUse(s.halfOpenPermitsInUse - 1);
            if (ref.compareAndSet(s, next)) {
                return;
            }
        }
    }

    public CircuitState currentState() {
        return ref.get().state;
    }

    public BreakerSnapshot snapshot() {
        State s = ref.get();
        return new BreakerSnapshot(name, s.state, s.consecutiveFailures, s.halfOpenSuccesses,
                s.halfOpenPermitsInUse, s.openedAtMs);
    }

    /** 不可变状态载体 */
    private static final class State {
        final CircuitState state;
        final int consecutiveFailures;
        final int halfOpenSuccesses;
        final int halfOpenPermitsInUse;
        final long openedAtMs;

        private State(CircuitState state, int consecutiveFailures, int halfOpenSuccesses,
                      int halfOpenPermitsInUse, long openedAtMs) {
            this.state = state;
            this.consecutiveFailures = consecutiveFailures;
            this.halfOpenSuccesses = halfOpenSuccesses;
            this.halfOpenPermitsInUse = halfOpenPermitsInUse;
            this.openedAtMs = openedAtMs;
        }

        static State closed() {
            return new State(CircuitState.CLOSED, 0, 0, 0, 0L);
        }

        static State openNow() {
            return new State(CircuitState.OPEN, 0, 0, 0, System.currentTimeMillis());
        }

        static State toHalfOpen(State s) {
            return new State(CircuitState.HALF_OPEN, 0, 0, 0, s.openedAtMs);
        }

        boolean isCooledDown(long openStateMs) {
            return System.currentTimeMillis() - openedAtMs >= openStateMs;
        }

        State withConsecutiveFailures(int v) {
            return new State(state, v, halfOpenSuccesses, halfOpenPermitsInUse, openedAtMs);
        }

        State withHalfOpenSuccesses(int v) {
            return new State(state, consecutiveFailures, v, halfOpenPermitsInUse, openedAtMs);
        }

        State withPermitsInUse(int v) {
            return new State(state, consecutiveFailures, halfOpenSuccesses, v, openedAtMs);
        }
    }
}
