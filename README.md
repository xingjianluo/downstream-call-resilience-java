# 多下游调用编排与弹性能力（downstream-call-resilience-java bootstrap-b）

在 Spring Boot 工程内实现一套面向多个下游依赖的调用编排能力，并用**纯本地、可复现**的方式模拟下游的
延迟、连接失败、错误响应、写确认与重复响应。各类失败的返回结果与熔断状态变化均可预期、可解释，
且不改动既有对外接口契约（仅新增 `/api/resilience/**` 路径）。

## 1. 能力总览

| 需求 | 实现位置 |
| --- | --- |
| 按下游分别生效的超时 / 重试 / 熔断策略 | `resilience/ResiliencePolicy`、`config/ResilienceProperties` |
| 失败归类：超时 / 连接失败 / 下游错误 / 熔断快速失败 | `resilience/FailureCategory` |
| 熔断打开后快速失败、不等待 | `resilience/CircuitBreaker`（OPEN 直接不发许可） |
| 重试不重复执行已确认写 | `resilience/FailureRules` + `mock/MockDownstreamServer` 幂等去重 |
| 熔断并发安全：半开有限探测、按下游隔离、计数不重不漏 | `resilience/CircuitBreaker`、`CircuitBreakerRegistry` |
| 请求级降级与部分结果，标明缺失与原因 | `orchestration/Orchestrator`、`PartResult`、`AggregatedResponse` |
| 重试与降级对同一副作用判定一致 | `FailureRules#decideRetry` 与 `#isSideEffectAmbiguous` 共用一份规则 |
| 本地模拟延迟/故障/重复响应 | `mock/MockDownstreamServer`（脚本行为 + 每键串行化去重表） |

## 2. 策略配置

在 `src/main/resources/application.properties` 中按下游名称配置，缺省值见 `ResiliencePolicy#defaults`：

```properties
resilience.downstreams.<下游名>.timeout-millis=200        # 单次尝试超时
resilience.downstreams.<下游名>.max-attempts=2            # 最大尝试次数（含首次）
resilience.downstreams.<下游名>.retry-backoff-millis=20   # 重试间隔
resilience.downstreams.<下游名>.failure-threshold=5       # 连续失败熔断阈值
resilience.downstreams.<下游名>.open-state-millis=2000    # OPEN 持续时长，到期进入半开
resilience.downstreams.<下游名>.half-open-permits=1       # 半开探测名额
resilience.downstreams.<下游名>.operation-kind=READ       # READ / IDEMPOTENT_WRITE / NON_IDEMPOTENT_WRITE
```

### 操作语义（operation-kind）

- `READ`：无副作用。连接失败、超时、下游错误均可重试。
- `IDEMPOTENT_WRITE`：请求携带幂等键，下游按幂等键去重；超时/错误可安全重试
  （首次若已迟到确认，重试拿到的是 `duplicate=true` 的重复响应，业务写不重复执行）。
- `NON_IDEMPOTENT_WRITE`：无幂等保护。一旦结果未知（超时或下游错误且未确认）**禁止重试**，
  降级结果按"副作用未知"标记。已被下游确认生效（`confirmed=true`）后即使收到错误响应也绝不重试。

## 3. 熔断状态语义

```
CLOSED ──连续失败达 failure-threshold──▶ OPEN
OPEN   ──open-state-millis 到期后首个请求──▶ HALF_OPEN（发放第 1 个探测许可）
HALF_OPEN ──所有探测成功──▶ CLOSED（清零统计）
HALF_OPEN ──任一探测失败──▶ OPEN（重置打开计时）
```

- **OPEN**：`tryAcquire()` 立即返回空，调用方不等待，结果类别 `CIRCUIT_OPEN`。
- **HALF_OPEN**：最多放行 `half-open-permits` 个探测，名额之外的请求快速失败；
  探测全部回收后才迁移，避免在途请求导致的提前跳转。
- **并发安全**：状态为不可变快照，迁移在同一监视器内串行化；每次迁移代际（generation）+1，
  许可绑定发放时代际且一次性消费（consumed），迟到结果不会污染新代际，并发计数不重复、不丢失。
- **下游隔离**：熔断器按名称在 `CircuitBreakerRegistry` 中分别维护，一个下游熔断不连带其他下游。

## 4. 失败类别（FailureCategory，可区分、可解释）

| 类别 | 触发条件 | 是否计入熔断失败 |
| --- | --- | --- |
| `TIMEOUT` | 单次尝试超过该下游 `timeout-millis`；或编排超过整体时限 | 是 |
| `CONNECTION_FAILED` | 模拟连接失败（请求未到达下游） | 是 |
| `DOWNSTREAM_ERROR` | 下游返回错误响应/异常 | 是 |
| `CIRCUIT_OPEN` | 熔断打开或半开名额已满，快速失败 | 否（请求未发出） |
| `REJECTED` | 无可用容量/线程中断 | 否 |
| `SUCCESS` | 成功 | 成功会清零连续失败 |

每次尝试都会打印日志，例如：

```
[inventory-service] attempt=3 类别=DOWNSTREAM_ERROR 熔断记账=失败 判定=终止
  依据=已达最大尝试次数 3，停止重试 状态=inventory-service[OPEN generation=1 recoverIn=800ms]
```

## 5. 编排与降级

`Orchestrator#aggregate(parts, overallDeadline)`：

1. 各分片在虚拟线程上并发调用（每个分片内部仍受该下游自己的超时/重试/熔断保护）；
2. 整体时限到期时，未完成分片直接按 `TIMEOUT` 降级，响应仍在时限内返回；
3. 每个失败分片回填降级值，`PartResult` 标注 `present/degraded/category/attempts/reason`；
4. `AggregatedResponse` 汇总 `complete`、`missing`（含下游名、类别、原因）与耗时。

## 6. 验证方法

```bash
# 全部 23 个单测（含 Spring 上下文装载）
mvn test

# 按场景单独运行
mvn test -Dtest=CircuitBreakerTest,CircuitHalfOpenTest,CircuitConcurrencyTest  # 熔断/半开/并发
mvn test -Dtest=RetryAndTimeoutTest                                            # 超时/重试/分类
mvn test -Dtest=DuplicateWriteTest                                             # 重复写/幂等去重
mvn test -Dtest=OrchestrationDegradeTest                                       # 降级/部分结果/整体时限
```

测试日志（控制台 + `target/surefire-reports`）会打印**每个下游的调用结果与判定依据**。

端到端演示：

```bash
mvn spring-boot:run
curl -s http://localhost:<port>/api/resilience/aggregate
# inventory-service 持续故障：第 1 次返回 DOWNSTREAM_ERROR(attempts=3)，
# 随后请求返回 CIRCUIT_OPEN 快速失败（~20ms），其余两个下游始终完整；
# order-write-service 携带幂等键 idem-demo-001，重复调用返回 duplicate=true 且写只执行一次。
curl -s -X POST http://localhost:<port>/api/resilience/demo/reset   # 重置模拟脚本
```

## 7. 测试场景覆盖一览

| 测试类 | 覆盖点 |
| --- | --- |
| `CircuitBreakerTest` | 阈值打开、OPEN 快速失败不阻塞、成功清零、下游隔离 |
| `CircuitHalfOpenTest` | 到期才半开、探测名额受限、探测成功关闭、探测失败重开并重置计时、2 名额组合 |
| `CircuitConcurrencyTest` | 20 线程并发失败恰好打开一次、打开后 100 并发请求全部快速失败无穿透 |
| `RetryAndTimeoutTest` | 连接失败/下游错误/真实超时分类、重试次数准确、非幂等写超时不重试、已确认写不重试 |
| `DuplicateWriteTest` | 迟到确认+重试只写一次、同键重复响应回放、32 并发同键只写一次、无键非幂等不享受保护 |
| `OrchestrationDegradeTest` | 部分结果按时返回、缺失标注与降级值、整体时限截断慢下游、写降级副作用判定一致 |

## 8. 主要代码结构

```
resilience/   FailureCategory / CircuitState / OperationKind / RetryDecision
              ResiliencePolicy        策略值对象
              FailureRules            唯一的"是否重试/副作用是否未知"判定来源
              CircuitBreaker          并发安全状态机（代际许可 + 不可变快照）
              CircuitBreakerRegistry  按下游隔离
              ResilienceClient        超时(Future 限时) + 重试 + 熔断的整合执行
mock/         MockDownstreamServer    脚本编排延迟/故障 + 幂等键去重(每键串行化)
orchestration/Orchestrator            并发编排 + 整体时限 + 降级部分结果
config/       ResilienceConfig/Properties  Bean 装配与配置绑定
web/          DemoController          /api/resilience/aggregate 演示端点
```
