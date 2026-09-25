package com.github.xingjianluo.downstreamcallresiliencejava.mock;

/**
 * 下游响应。{@code confirmed} 表示写操作已被下游确认生效；
 * {@code duplicate} 表示这是同一幂等键的重复响应（写只执行一次）。
 */
public record DownstreamResponse(String body, int status, boolean confirmed, boolean duplicate) {

    public static DownstreamResponse ok(String body) {
        return new DownstreamResponse(body, 200, false, false);
    }

    public static DownstreamResponse confirmedWrite(String body) {
        return new DownstreamResponse(body, 200, true, false);
    }
}
