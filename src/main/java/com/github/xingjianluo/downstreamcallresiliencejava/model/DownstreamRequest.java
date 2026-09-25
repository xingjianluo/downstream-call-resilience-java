package com.github.xingjianluo.downstreamcallresiliencejava.model;

/**
 * 对单个下游的调用请求。
 */
public class DownstreamRequest {

    private final String downstream;
    private final Object payload;
    private final boolean write;
    private final String idempotencyKey;

    public DownstreamRequest(String downstream, Object payload, boolean write, String idempotencyKey) {
        this.downstream = downstream;
        this.payload = payload;
        this.write = write;
        this.idempotencyKey = idempotencyKey;
    }

    public static DownstreamRequest read(String downstream, Object payload) {
        return new DownstreamRequest(downstream, payload, false, null);
    }

    public static DownstreamRequest write(String downstream, Object payload, String idempotencyKey) {
        return new DownstreamRequest(downstream, payload, true, idempotencyKey);
    }

    public String getDownstream() { return downstream; }
    public Object getPayload() { return payload; }
    public boolean isWrite() { return write; }
    public String getIdempotencyKey() { return idempotencyKey; }
}
