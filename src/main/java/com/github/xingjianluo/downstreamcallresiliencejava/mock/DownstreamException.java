package com.github.xingjianluo.downstreamcallresiliencejava.mock;

import com.github.xingjianluo.downstreamcallresiliencejava.resilience.FailureCategory;

/**
 * 模拟下游产生的可分类失败。
 */
public class DownstreamException extends RuntimeException {

    private final FailureCategory category;
    private final boolean sideEffectConfirmed;

    public DownstreamException(FailureCategory category, String message) {
        this(category, message, false);
    }

    public DownstreamException(FailureCategory category, String message, boolean sideEffectConfirmed) {
        super(message);
        this.category = category;
        this.sideEffectConfirmed = sideEffectConfirmed;
    }

    public FailureCategory category() {
        return category;
    }

    public boolean sideEffectConfirmed() {
        return sideEffectConfirmed;
    }
}
