package com.github.xingjianluo.downstreamcallresiliencejava.web;

/**
 * 编排请求中的单个下游调用。
 */
public class CallDto {

    private String downstream;
    private Object payload;
    /** true 表示写操作；默认 false（读） */
    private boolean write = false;
    /** 写操作建议提供；缺失时视为非幂等写（不重试） */
    private String idempotencyKey;

    public String getDownstream() { return downstream; }
    public void setDownstream(String downstream) { this.downstream = downstream; }
    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }
    public boolean isWrite() { return write; }
    public void setWrite(boolean write) { this.write = write; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
}
