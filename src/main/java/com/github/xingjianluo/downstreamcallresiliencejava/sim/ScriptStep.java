package com.github.xingjianluo.downstreamcallresiliencejava.sim;

/**
 * 模拟脚本中的一步，按调用序号依次生效；脚本用完后循环最后一步。
 */
public record ScriptStep(
        StepKind kind,
        long delayMs,
        Object value,
        String errorCode) {

    public static ScriptStep ok(Object value) {
        return new ScriptStep(StepKind.OK, 0, value, null);
    }

    public static ScriptStep latency(Object value, long delayMs) {
        return new ScriptStep(StepKind.LATENCY, delayMs, value, null);
    }

    public static ScriptStep slow(long delayMs, Object value) {
        return new ScriptStep(StepKind.SLOW, delayMs, value, null);
    }

    public static ScriptStep connectFailure(long delayMs) {
        return new ScriptStep(StepKind.CONNECT_FAILURE, delayMs, null, "CONN_REFUSED");
    }

    public static ScriptStep error(long delayMs, String errorCode) {
        return new ScriptStep(StepKind.ERROR_RESPONSE, delayMs, null, errorCode);
    }

    public static ScriptStep responseLost(long delayMs) {
        return new ScriptStep(StepKind.RESPONSE_LOST, delayMs, null, "RESPONSE_LOST");
    }

    public static ScriptStep duplicate(Object value, long delayMs) {
        return new ScriptStep(StepKind.DUPLICATE_RESPONSE, delayMs, value, null);
    }
}
