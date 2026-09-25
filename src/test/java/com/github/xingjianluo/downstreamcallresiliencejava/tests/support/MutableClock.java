package com.github.xingjianluo.downstreamcallresiliencejava.tests.support;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * 测试用可控时钟：熔断打开时长、半开恢复时机可精确推进，不依赖真实睡眠。
 */
public class MutableClock extends Clock {

    private long millis = 1_000_000L;

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis);
    }

    @Override
    public long millis() {
        return millis;
    }

    public void advanceMillis(long delta) {
        this.millis += delta;
    }
}
