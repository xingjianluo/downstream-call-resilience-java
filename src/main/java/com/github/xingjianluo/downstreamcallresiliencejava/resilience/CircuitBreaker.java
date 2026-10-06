package com.github.xingjianluo.downstreamcallresiliencejava.resilience;

import java.time.Clock;

/**
 * 单个下游的熔断器，状态迁移并发安全。
 * <p>状态机：
 * <pre>
 *   CLOSED --连续失败达阈值--> OPEN --openStateDuration 到期--> HALF_OPEN
 *   HALF_OPEN --探测全部成功--> CLOSED
 *   HALF_OPEN --任一探测失败--> OPEN（重置打开计时，代际 +1）
 * </pre>
 * 实现要点：
 * <ul>
 *   <li>所有状态迁移在同一把监视器上串行化，状态本身为不可变快照，
 *       多线程并发触发迁移时每次成功/失败恰好计数一次，不会重复或丢失；</li>
 *   <li>每次迁移代际（generation）递增，许可（{@link Permit}）绑定发放时的代际，
 *       上一代际迟到的结果不会污染新一代际统计；</li>
 *   <li>半开状态下最多发放 {@code halfOpenPermits} 个探测许可，其余请求快速失败；</li>
 *   <li>OPEN 状态 tryAcquire 立即返回 null，调用方据此快速失败而不等待。</li>
 * </ul>
 */
public class CircuitBreaker {

    private final String name;
    private final ResiliencePolicy policy;
    private final Clock clock;

    private State state;

    public CircuitBreaker(String name, ResiliencePolicy policy, Clock clock) {
        this.name = name;
        this.policy = policy;
        this.clock = clock;
        this.state = new Closed(0);
    }

    public String name() {
        return name;
    }

    public synchronized ResiliencePolicy policy() {
        return policy;
    }

    public synchronized CircuitState state() {
        return state.kind();
    }

    /**
     * 只读：当前连续失败计数（HALF_OPEN 下记已失败的探测数）。
     */
    public synchronized int consecutiveFailures() {
        if (state instanceof Closed closed) {
            return closed.failures();
        }
        if (state instanceof HalfOpen half) {
            return half.failed();
        }
        return 0;
    }

    /**
     * 只读：OPEN 状态下距半开探测的剩余毫秒；非 OPEN 返回 0。
     */
    public synchronized long recoverInMillis() {
        if (state instanceof Open open) {
            return Math.max(0, policy.openStateDuration().toMillis()
                    - (clock.millis() - open.openedAt()));
        }
        return 0;
    }

    /**
     * 尝试获取调用许可。
     *
     * @return 许可；OPEN（未到探测时机）或 HALF_OPEN 名额已满时返回 {@code null}，调用方应快速失败
     */
    public synchronized Permit tryAcquire() {
        long now = clock.millis();
        if (state instanceof Open open) {
            if (now - open.openedAt() < policy.openStateDuration().toMillis()) {
                return null;
            }
            // 打开时间到期：迁移到半开，发放第一个探测许可
            long generation = open.generation() + 1;
            state = new HalfOpen(generation, 1, 1, 0, 0);
            return new Permit(generation, true);
        }
        if (state instanceof HalfOpen half) {
            if (half.permitsGiven() >= policy.halfOpenPermits()) {
                return null;
            }
            state = new HalfOpen(half.generation(), half.permitsGiven() + 1,
                    half.inFlight() + 1, half.succeeded(), half.failed());
            return new Permit(half.generation(), true);
        }
        // CLOSED：普通许可，绑定当前代际
        return new Permit(((Closed) state).generation(), false);
    }

    public synchronized void recordSuccess(Permit permit) {
        if (permit == null || permit.consumed || !validGeneration(permit)) {
            return;
        }
        permit.consumed = true;
        if (permit.probe) {
            HalfOpen half = (HalfOpen) state;
            completeProbe(new HalfOpen(half.generation(), half.permitsGiven(),
                    half.inFlight() - 1, half.succeeded() + 1, half.failed()));
        } else if (state instanceof Closed closed) {
            if (closed.failures() != 0) {
                // 成功只清零连续失败，保持代际不变，使在途普通许可仍然有效
                state = new Closed(0, closed.generation());
            }
        }
    }

    public synchronized void recordFailure(Permit permit, FailureCategory category) {
        if (permit == null || permit.consumed || !validGeneration(permit)) {
            return;
        }
        permit.consumed = true;
        if (permit.probe) {
            HalfOpen half = (HalfOpen) state;
            HalfOpen next = new HalfOpen(half.generation(), half.permitsGiven(),
                    half.inFlight() - 1, half.succeeded(), half.failed() + 1);
            state = next;
            completeProbe(next);
            return;
        }
        if (state instanceof Closed closed) {
            int failures = closed.failures() + 1;
            if (failures >= policy.failureThreshold()) {
                state = new Open(clock.millis(), closed.generation() + 1);
            } else {
                state = new Closed(failures, closed.generation());
            }
        }
    }

    /**
     * 探测许可全部发放且全部回收时，根据探测结果决定回到 CLOSED 还是重新 OPEN。
     */
    private void completeProbe(HalfOpen half) {
        if (state != half) {
            state = half;
        }
        if (half.inFlight() != 0 || half.permitsGiven() < policy.halfOpenPermits()) {
            return;
        }
        if (half.failed() > 0) {
            // 探测失败：重新打开并重置计时；不影响其他下游（熔断器按下游隔离）
            state = new Open(clock.millis(), half.generation() + 1);
        } else {
            // 探测全部成功：关闭并清零统计，沿用半开代际以兼容在途普通许可
            state = new Closed(0, half.generation());
        }
    }

    private boolean validGeneration(Permit permit) {
        long current = switch (state) {
            case Closed closed -> closed.generation();
            case Open open -> open.generation();
            case HalfOpen half -> half.generation();
        };
        return permit.generation == current;
    }

    /**
     * 仅供测试/日志查看的快照。
     */
    public synchronized String snapshot() {
        if (state instanceof Closed closed) {
            return name + "[CLOSED failures=" + closed.failures()
                    + " threshold=" + policy.failureThreshold() + "]";
        }
        if (state instanceof Open open) {
            long waitMillis = policy.openStateDuration().toMillis()
                    - (clock.millis() - open.openedAt());
            return name + "[OPEN generation=" + open.generation()
                    + " recoverIn=" + Math.max(0, waitMillis) + "ms]";
        }
        HalfOpen half = (HalfOpen) state;
        return name + "[HALF_OPEN generation=" + half.generation()
                + " permitsGiven=" + half.permitsGiven() + "/" + policy.halfOpenPermits()
                + " inFlight=" + half.inFlight()
                + " succeeded=" + half.succeeded() + " failed=" + half.failed() + "]";
    }

    /**
     * 调用许可：绑定发放时的代际；{@code consumed} 保证每个许可只被记账一次。
     */
    public static final class Permit {
        final long generation;
        final boolean probe;
        boolean consumed;

        Permit(long generation, boolean probe) {
            this.generation = generation;
            this.probe = probe;
        }

        public boolean isProbe() {
            return probe;
        }
    }

    private sealed interface State permits Closed, Open, HalfOpen {
        CircuitState kind();
    }

    private record Closed(int failures, long generation) implements State {
        Closed(int failures) {
            this(failures, 0);
        }

        @Override
        public CircuitState kind() {
            return CircuitState.CLOSED;
        }
    }

    private record Open(long openedAt, long generation) implements State {
        @Override
        public CircuitState kind() {
            return CircuitState.OPEN;
        }
    }

    private record HalfOpen(long generation, int permitsGiven, int inFlight,
                            int succeeded, int failed) implements State {
        @Override
        public CircuitState kind() {
            return CircuitState.HALF_OPEN;
        }
    }
}
