package com.github.xingjianluo.downstreamcallresiliencejava.web;

import java.util.List;

/**
 * 对外编排请求体。
 */
public class OrchestrationRequestDto {

    private String requestId;
    private List<CallDto> calls;
    /** 整体约定时限（毫秒）；为空时使用服务端默认配置 */
    private Long deadlineMs;

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public List<CallDto> getCalls() { return calls; }
    public void setCalls(List<CallDto> calls) { this.calls = calls; }
    public Long getDeadlineMs() { return deadlineMs; }
    public void setDeadlineMs(Long deadlineMs) { this.deadlineMs = deadlineMs; }
}
