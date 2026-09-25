# downstream-call-resilience-java

面向多个下游依赖的调用编排与弹性能力。使用**本地模拟器**复现下游的延迟、连接失败、
错误响应、响应丢失与重复响应，无需任何真实外部依赖，即可让各类失败场景的返回结果与
熔断状态变化可预期、可解释。

## 1. 能力总览

| 能力 | 说明 |
| --- | --- |
| 按下游独立的超时 | 每次尝试受 `timeout-ms` 与整体 `deadline-ms` 的双重约束 |
| 按下游独立的重试 | 读 / 幂等写分别配置尝试次数；非幂等写永不重试 |
| 按下游独立的熔断 | CLOSED / OPEN / HALF_OPEN 三态，CAS 并发安全，故障不连带其他下游 |
| 可区分失败类别 | `TIMEOUT` / `CONNECT_FAILURE` / `DOWNSTREAM_ERROR` / `CIRCUIT_OPEN` / `RESPONSE_LOST` |
| 幂等与副作用台账 | 已确认生效的写不会因重试再次执行；重试路径与降级路径共用同一判定来源 |
| 请求级降级 | 部分下游不可用时整体在约定时限内返回 200，缺失部分标注原因 |
| 可解释日志 | 每个下游的每次尝试、最终结果、熔断状态与“判定依据”均打印 |

## 2. 失败类别语义（FailureKind）

| 类别 | 含义 | 是否可重试 |
| --- | --- | --- |
| `TIMEOUT` | 在超时时间内未收到下游响应 | 读/幂等写可重试（受时限约束） |
| `CONNECT_FAILURE` | 连接未建立，请求未到达下游，**无副作用** | 读/幂等写可重试 |
| `DOWNSTREAM_ERROR` | 下游返回了错误响应（如 5xx / 业务错误） | 读/幂等写可重试 |
| `CIRCUIT_OPEN` | 熔断打开或半开探测位已满，**快速失败不等待** | 否（不触达下游） |
| `RESPONSE_LOST` | 写操作服务端可能已执行，但响应丢失（语义不明） | **否**，避免重复写 |

## 3. 熔断器状态语义（CircuitBreaker）

```
              连续失败 >= failureThreshold
   CLOSED ───────────────────────────────► OPEN
     ▲                                       │
     │ 半开探测连续成功 >= halfOpenSuccess    │ 冷却 openStateMs 到期
     │                                       ▼
     └────────────────────────────────── HALF_OPEN
              任一探测失败 ──► 立即回到 OPEN（重置冷却计时）
```

- **CLOSED**：请求正常通过；记录连续失败数，达到阈值进入 OPEN。
- **OPEN**：所有请求立即返回 `CIRCUIT_OPEN`，不发起底层调用、不等待。
- **HALF_OPEN**：冷却到期后由恰好一个线程通过 CAS 迁入；只允许 `halfOpenPermits`
  个探测请求同时在途，其余仍快速失败；探测成功计数达标后关闭，任一失败立即重开。
- **并发安全**：状态为不可变对象，所有迁移走 `AtomicReference.compareAndSet` 自旋，
  计数只在 CAS 成功时变更，因此并发触发迁移不会重复计数或丢失计数（见
  `CircuitBreakerConcurrencyTest`，64 线程并发迁移，半开在途许可峰值始终 ≤ 配置值）。
- **故障隔离**：每个下游一个熔断器实例，A 下游打开绝不影响 B 下游。
- **记账口径**：每次“逻辑调用”只向熔断器记账一次，不随内部重试次数膨胀。

## 4. 写操作与副作用一致性

- **非幂等写**（无 idempotencyKey）：最多投递一次，任何失败都不重试，防止“下游已生效
  但响应丢失”时重复执行。
- **幂等写**（有 idempotencyKey）：
  - 下游服务端模拟器按 key 去重，重复投递只生效一次并回放首次结果；
  - 调用方在 `SideEffectLedger`（唯一事实来源）中登记“语义不明 / 已确认”，
    **每次尝试前都先查台账**，已确认生效则直接复用结果，不再发起调用；
  - 降级返回时同样读取该台账标注 `sideEffectConfirmed`，因此重试与降级判定口径一致；
  - `RESPONSE_LOST` 被标记为语义不明且不盲目重试。
- **重复响应**：同一次调用收到两次响应时折叠为一次成功，并在结果中置
  `duplicateResponse=true`。

## 5. 对外接口契约（保持稳定）

`POST /api/orchestrate`

请求：
```json
{
  "requestId": "r1",
  "deadlineMs": 600,
  "calls": [
    {"downstream": "inventory", "payload": "sku-1", "write": false},
    {"downstream": "orders", "payload": {"id": 1}, "write": true, "idempotencyKey": "k-1"}
  ]
}
```

响应（**即使部分下游降级，HTTP 也返回 200**，结构恒定）：
```json
{
  "success": false,
  "elapsedMs": 103,
  "deadlineMs": 600,
  "parts": [
    {"downstream": "inventory", "status": "OK", "value": "item-1001",
     "reason": null, "reasonMessage": null, "attempts": 1, "elapsedMs": 2,
     "sideEffectConfirmed": false},
    {"downstream": "pricing", "status": "DEGRADED", "value": null,
     "reason": "TIMEOUT",
     "reasonMessage": "下游在超时时间内未返回（TIMEOUT_120ms），已尝试 2 次",
     "attempts": 2, "elapsedMs": 250, "sideEffectConfirmed": false}
  ],
  "degradedDownstreams": ["pricing"],
  "summary": "DEGRADED missing=[pricing] withinDeadlineMs=600"
}
```

## 6. 配置说明（application.properties）

按下游分别配置策略（缺省回退到 `DownstreamPolicy` 默认值）：

```properties
resilience.orchestration-deadline-ms=600          # 整体编排时限

resilience.policies.<下游名>.timeout-ms=120              # 单次尝试超时
resilience.policies.<下游名>.read-max-attempts=2         # 读最大尝试次数(含首次)
resilience.policies.<下游名>.write-max-attempts=2        # 幂等写最大尝试次数
resilience.policies.<下游名>.failure-threshold=3         # 连续失败熔断阈值
resilience.policies.<下游名>.open-state-ms=500           # OPEN 冷却时长
resilience.policies.<下游名>.half-open-permits=2         # 半开并发探测许可数
resilience.policies.<下游名>.half-open-success-threshold=2
resilience.policies.<下游名>.retry-backoff-ms=10         # 重试退避
```

本地模拟脚本（按底层投递序号依次生效，用完后循环最后一步）：

```properties
resilience.sim.downstreams.<下游名>.idempotency-aware=true
resilience.sim.downstreams.<下游名>.steps[0].kind=SLOW
resilience.sim.downstreams.<下游名>.steps[0].delay-ms=500
resilience.sim.downstreams.<下游名>.steps[0].value=price-99
```

`kind` 取值：`OK`、`LATENCY`（延迟后成功）、`SLOW`（慢于超时）、
`CONNECT_FAILURE`、`ERROR_RESPONSE`（可配 `error-code`）、
`RESPONSE_LOST`、`DUPLICATE_RESPONSE`。

## 7. 验证方法

```bash
mvn test
```

| 测试类 | 覆盖场景 |
| --- | --- |
| `FailureKindTest` | 超时 / 连接失败 / 下游错误的类别可区分，重试次数正确 |
| `CircuitBreakerOpenTest` | 达到阈值打开；打开后快速失败、不触达下游、零等待；健康下游不被连带 |
| `CircuitBreakerHalfOpenTest` | 半开有限探测、成功恢复、探测失败立即重开 |
| `CircuitBreakerConcurrencyTest` | 200 线程并发记账计数精确；64 线程并发迁移只迁一次、许可不超额 |
| `DuplicateWriteTest` | 幂等写重试只执行一次、非幂等写不重试、重复响应去重、RESPONSE_LOST 不重试 |
| `OrchestrationDegradeTest` | 部分降级按时返回、原因可区分、副作用判定一致、熔断快速失败 |
| `OrchestrationControllerTest` | HTTP 200 + 部分结果结构契约、空请求 400 |

手动验证（启动后端口随机，查看日志；可用 `server.port=8080` 固定）：

```bash
mvn spring-boot:run
curl -s -X POST localhost:8080/api/orchestrate \
  -H 'Content-Type: application/json' \
  -d '{"deadlineMs":600,"calls":[{"downstream":"inventory","payload":"sku"},{"downstream":"pricing","payload":"sku"}]}'
```

测试与运行日志会为每个下游打印类似下面的行，含结果与“判定依据”：

```
[pricing] result=FAIL kind=TIMEOUT code=TIMEOUT_120ms attempt=1 basis=可重试失败，退避10ms 后重试
[pricing] final=FAIL kind=TIMEOUT attempts=2 circuit=CLOSED basis=最终失败：重试已用尽，熔断计一次失败
```

## 8. 主要代码结构

```
model/          FailureKind / CircuitState 相关结果模型
policy/         DownstreamPolicy + PolicyRegistry（按下游独立）
breaker/        CircuitBreaker（CAS 三态机）+ BreakerSnapshot
transport/      DownstreamTransport 抽象 + TransportOutcome
sim/            SimulatedDownstreamTransport（本地故障/延迟/重复响应复现）
idempotency/    SideEffectLedger（副作用唯一事实来源）
caller/         DownstreamCaller（超时→熔断→重试→副作用判定）
orchestrator/   Orchestrator（并行、统一时限、部分降级）
web/            POST /api/orchestrate
config/         策略/脚本配置绑定与 Bean 装配
```
