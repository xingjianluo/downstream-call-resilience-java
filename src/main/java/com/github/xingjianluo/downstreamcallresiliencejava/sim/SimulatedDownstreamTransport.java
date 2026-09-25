package com.github.xingjianluo.downstreamcallresiliencejava.sim;

import com.github.xingjianluo.downstreamcallresiliencejava.model.DownstreamRequest;
import com.github.xingjianluo.downstreamcallresiliencejava.transport.DownstreamTransport;
import com.github.xingjianluo.downstreamcallresiliencejava.transport.TransportOutcome;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地可复现的下游模拟器。
 * <p>
 * 每个下游持有一段有序脚本 {@link DownstreamSpec#script()}，按“服务端实际收到的调用序号”
 * 依次生效，脚本用完后循环最后一步。支持：
 * <ul>
 *   <li>{@link StepKind#LATENCY}：延迟后正常响应（验证超时判定）；</li>
 *   <li>{@link StepKind#SLOW}：处理时间长于调用方超时，调用方先放弃，下游稍后仍完成；</li>
 *   <li>{@link StepKind#CONNECT_FAILURE}：连接失败，请求不到达，无副作用；</li>
 *   <li>{@link StepKind#ERROR_RESPONSE}：下游错误响应；</li>
 *   <li>{@link StepKind#RESPONSE_LOST}：写操作服务端已执行，但响应丢失（语义不明）；</li>
 *   <li>{@link StepKind#DUPLICATE_RESPONSE}：同一次调用重复投递两次响应（重复响应）。</li>
 * </ul>
 * idempotencyAware 的下游在服务端按 idempotencyKey 去重：重复投递只生效一次并返回首次结果。
 */
public class SimulatedDownstreamTransport implements DownstreamTransport {

    private final Map<String, DownstreamSpec> specs;
    private final Map<String, AtomicInteger> rawDeliveries = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> serverExecutions = new ConcurrentHashMap<>();
    /** key = 下游#幂等键，value = 首次执行产生的结果 */
    private final Map<String, TransportOutcome> idempotencyTable = new ConcurrentHashMap<>();

    public SimulatedDownstreamTransport(Map<String, DownstreamSpec> specs) {
        this.specs = new HashMap<>(specs);
    }

    @Override
    public TransportOutcome invoke(DownstreamRequest request) throws InterruptedException {
        String name = request.getDownstream();
        DownstreamSpec spec = specs.get(name);
        if (spec == null) {
            throw new IllegalArgumentException("no simulated downstream named: " + name);
        }

        // 底层收到的每次投递都占一个序号（含被幂等表命中回放的重复投递），保证脚本按序可复现
        int deliveryIndex = rawDeliveries.computeIfAbsent(name, k -> new AtomicInteger()).getAndIncrement();

        String idempotencyKey = null;
        if (spec.idempotencyAware() && request.isWrite() && request.getIdempotencyKey() != null) {
            idempotencyKey = name + "#" + request.getIdempotencyKey();
            TransportOutcome cached = idempotencyTable.get(idempotencyKey);
            if (cached != null) {
                // 同一幂等键的重复投递：服务端不再执行副作用，回放首次结果
                if (cached.latencyMs() > 0) {
                    Thread.sleep(Math.min(cached.latencyMs(), 5));
                }
                return cached;
            }
        }

        ScriptStep step = pickStep(spec.script(), deliveryIndex);
        TransportOutcome outcome = executeStep(step, request.isWrite());

        // 只有真正到达服务端并产生副作用的步骤才计入“服务端执行次数”；
        // 连接失败请求不到达，不计入（但仍在上面的投递序号中占一位，保证脚本顺序正确）
        if (outcome.serverExecuted()) {
            serverExecutions.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
        }

        if (idempotencyKey != null && outcome.serverExecuted()) {
            // 首次执行：登记结果。若并发出现同 key 的首次投递，以先登记者为准
            TransportOutcome winner = ((ConcurrentHashMap<String, TransportOutcome>) idempotencyTable)
                    .putIfAbsent(idempotencyKey, outcome);
            if (winner != null) {
                return winner;
            }
        }
        return outcome;
    }

    private static ScriptStep pickStep(List<ScriptStep> script, int index) {
        if (script.isEmpty()) {
            return ScriptStep.ok("ok");
        }
        int i = Math.min(index, script.size() - 1);
        return script.get(i);
    }

    private TransportOutcome executeStep(ScriptStep step, boolean isWrite) throws InterruptedException {
        long delay = Math.max(0, step.delayMs());
        switch (step.kind()) {
            case OK -> {
                if (delay > 0) {
                    Thread.sleep(delay);
                }
                return TransportOutcome.ok(step.value(), delay, false, isWrite);
            }
            case LATENCY -> {
                Thread.sleep(delay);
                return TransportOutcome.ok(step.value(), delay, false, isWrite);
            }
            case SLOW -> {
                // 模拟“慢调用”：调用方会先超时；这里仍完成写副作用（针对写请求）
                Thread.sleep(delay);
                return TransportOutcome.ok(step.value(), delay, false, isWrite);
            }
            case CONNECT_FAILURE -> {
                if (delay > 0) {
                    Thread.sleep(delay);
                }
                return TransportOutcome.fail(
                        com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind.CONNECT_FAILURE,
                        step.errorCode(), delay, false);
            }
            case ERROR_RESPONSE -> {
                if (delay > 0) {
                    Thread.sleep(delay);
                }
                return TransportOutcome.fail(
                        com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind.DOWNSTREAM_ERROR,
                        step.errorCode(), delay, false);
            }
            case RESPONSE_LOST -> {
                // 服务端执行了写副作用，但响应在回程中丢失
                Thread.sleep(delay);
                return TransportOutcome.fail(
                        com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind.RESPONSE_LOST,
                        step.errorCode(), delay, true);
            }
            case DUPLICATE_RESPONSE -> {
                Thread.sleep(delay);
                // 同一次调用服务端执行一次，却在链路上投递了两次响应；调用方必须去重为一次成功
                return TransportOutcome.ok(step.value(), delay, true, isWrite);
            }
            default -> {
                return TransportOutcome.fail(
                        com.github.xingjianluo.downstreamcallresiliencejava.model.FailureKind.DOWNSTREAM_ERROR,
                        "UNSUPPORTED_STEP", 0, false);
            }
        }
    }

    /** 该下游副作用被实际执行的次数（按幂等键去重后），供测试断言 */
    public int serverExecutions(String downstream) {
        AtomicInteger c = serverExecutions.get(downstream);
        return c == null ? 0 : c.get();
    }

    /** 底层收到的调用尝试总数（含被幂等去重的重复投递），供测试断言 */
    public int rawDeliveries(String downstream) {
        AtomicInteger c = rawDeliveries.get(downstream);
        return c == null ? 0 : c.get();
    }

    public void resetCounters() {
        rawDeliveries.clear();
        serverExecutions.clear();
        idempotencyTable.clear();
    }
}
