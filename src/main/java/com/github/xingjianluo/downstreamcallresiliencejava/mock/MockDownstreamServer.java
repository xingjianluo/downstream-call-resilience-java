package com.github.xingjianluo.downstreamcallresiliencejava.mock;

import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地可复现的模拟下游服务器。
 * <p>每个下游分别拥有：
 * <ul>
 *   <li>一段可编排的脚本 {@link ScriptedBehavior}，按调用序号决定本次
 *       延迟多久、返回成功/错误、连接失败、超时，或"先确认写再返回错误"；</li>
 *   <li>独立的调用计数器（真实进入业务逻辑的次数，用于验证写只执行一次）；</li>
 *   <li>幂等键去重表：带幂等键的写操作确认后，相同幂等键的再次调用直接返回
 *       首次响应并标记 {@code duplicate=true}，业务逻辑不会再次执行——
 *       这就是"重复响应"的本地复现。</li>
 * </ul>
 */
public class MockDownstreamServer {

    private final Clock clock;
    private final ConcurrentMap<String, DownstreamState> downstreams = new ConcurrentHashMap<>();

    public MockDownstreamServer(Clock clock) {
        this.clock = clock;
    }

    /**
     * 为指定下游安装脚本行为；安装时重置该下游的计数与去重表。
     */
    public void script(String downstreamName, ScriptedBehavior behavior) {
        downstreams.put(downstreamName, new DownstreamState(behavior));
    }

    public DownstreamTransport transport() {
        return new TransportImpl();
    }

    public int invocationCount(String downstreamName) {
        DownstreamState state = downstreams.get(downstreamName);
        return state == null ? 0 : state.invocations.get();
    }

    public Map<String, Integer> invocationCounts() {
        Map<String, Integer> result = new LinkedHashMap<>();
        downstreams.forEach((name, state) -> result.put(name, state.invocations.get()));
        return result;
    }

    public void reset() {
        downstreams.clear();
    }

    private final class TransportImpl implements DownstreamTransport {
        @Override
        public DownstreamResponse invoke(String downstreamName, DownstreamRequest request) {
            DownstreamState state = downstreams.computeIfAbsent(downstreamName,
                    name -> new DownstreamState((index, req) -> DownstreamResponse.ok(name + "-ok")));

            String key = request.idempotencyKey();
            if (key == null) {
                return runWithoutDedupe(state, request);
            }
            // 同一幂等键的"检查-执行-登记"串行化，保证并发同键写只执行一次；
            // 不同幂等键/不同下游互不阻塞。
            Object keyLock = state.keyLocks.computeIfAbsent(key, k -> new Object());
            synchronized (keyLock) {
                DownstreamResponse cached = state.idempotencyStore.get(key);
                if (cached != null) {
                    // 命中：回放首次响应（重复响应），写不重复执行
                    return new DownstreamResponse(cached.body(), cached.status(),
                            cached.confirmed(), true);
                }
                DownstreamResponse response = runWithoutDedupe(state, request);
                if (response != null && response.confirmed()) {
                    state.idempotencyStore.put(key, response);
                }
                return response;
            }
        }

        private DownstreamResponse runWithoutDedupe(DownstreamState state, DownstreamRequest request) {
            int index = state.invocations.getAndIncrement();
            return state.behavior.behave(index, request);
        }
    }

    private static final class DownstreamState {
        final ScriptedBehavior behavior;
        final AtomicInteger invocations = new AtomicInteger();
        final ConcurrentMap<String, DownstreamResponse> idempotencyStore = new ConcurrentHashMap<>();
        final ConcurrentMap<String, Object> keyLocks = new ConcurrentHashMap<>();

        DownstreamState(ScriptedBehavior behavior) {
            this.behavior = behavior;
        }
    }

    /**
     * 便捷工厂：构造模拟延迟/故障（供测试与演示使用）。
     */
    public static DownstreamException connectionFailure(String message) {
        return new DownstreamException(FailureCategory.CONNECTION_FAILED, message);
    }

    public static DownstreamException timeoutFailure(String message) {
        return new DownstreamException(FailureCategory.TIMEOUT, message);
    }

    public static DownstreamException downstreamError(String message) {
        return new DownstreamException(FailureCategory.DOWNSTREAM_ERROR, message);
    }

    public static DownstreamException confirmedThenError(String message) {
        return new DownstreamException(FailureCategory.DOWNSTREAM_ERROR, message, true);
    }
}
