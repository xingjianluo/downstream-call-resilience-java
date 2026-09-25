package com.github.xingjianluo.downstreamcallresiliencejava.sim;

/**
 * 模拟器单步脚本的行为类型。
 */
public enum StepKind {
    /** 正常响应 */
    OK,
    /** 延迟指定毫秒后响应 */
    LATENCY,
    /** 连接失败（请求不到达下游） */
    CONNECT_FAILURE,
    /** 下游返回错误响应 */
    ERROR_RESPONSE,
    /** 处理慢于超时，最终给出响应（调用方会先超时） */
    SLOW,
    /** 写操作服务端执行成功，但响应丢失 */
    RESPONSE_LOST,
    /** 服务端重复投递响应（同一调用收到两次响应） */
    DUPLICATE_RESPONSE
}
