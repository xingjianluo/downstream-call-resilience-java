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
| 读失败优先回退最近一次成功结果（标注旧数据与新鲜度） | `fallback/FallbackCache`、`Orchestrator#fallbackPart` |
| 回退数据容量上限 + 有效期 + LRU 淘汰，O(1) 查找/淘汰 | `fallback/FallbackCache`、`FallbackPolicy` |
| 写操作绝不回退旧数据（与禁重试同一份判定） | `FailureRules#isStaleFallbackAllowed` |
| 分片标注取值来源（实时/旧数据回退/静态兜底） | `orchestration/PartSource`、`PartResult#source/ageMillis` |
| 只读查询各下游熔断状态与回退缓存占用 | `GET /api/resilience/status`、`CircuitBreakerRegistry#snapshots`、`FallbackCacheRegistry#stats` |
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
resilience.downstreams.<下游名>.fallback-capacity=64      # 回退缓存条数上限（默认 64）
resilience.downstreams.<下游名>.fallback-ttl-millis=60000 # 回退数据有效期（默认 60s）
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
3. 每个失败分片先尝试旧数据回退（见第 6 节），不可用时才回填静态缺省值，
   `PartResult` 标注 `present/degraded/category/attempts/reason`，
   并用 `source`（`LIVE` / `STALE_FALLBACK` / `STATIC_FALLBACK`）标明取值来源、
   用 `ageMillis` 标明旧数据新鲜度（仅旧数据回退时有值）；
4. `AggregatedResponse` 汇总 `complete`、`missing`（含下游名、类别、原因）与耗时。

## 6. 旧数据回退（Fallback Cache）

读操作失败要走降级时，优先回退该下游**最近一次成功的结果**，并在分片里标清
`source=STALE_FALLBACK` 与 `ageMillis`（数据距成功写入的时长）。满足以下任一条件时
才退回原来的静态缺省值（`source=STATIC_FALLBACK`）：

- 该下游从未成功过（缓存未命中）；
- 旧数据已超过有效期（`fallback-ttl-millis`，到期即不可回退）；
- 该操作不适合回退旧数据：所有写操作（`IDEMPOTENT_WRITE` / `NON_IDEMPOTENT_WRITE`）
  绝不回退旧数据。该判定由 `FailureRules#isStaleFallbackAllowed` 给出，
  与 `decideRetry` 的禁重试、`isSideEffectAmbiguous` 的副作用未知判定共用同一份规则——
  不会出现"一边判定不能重发、另一边却把旧结果当成功返回"的矛盾。

回退缓存的约束（`fallback/FallbackCache`，按下游隔离）：

- **只缓存成功结果**：写入只发生在成功路径，失败结果绝不进缓存；
- **容量上限 + 明确淘汰规则**：超过 `fallback-capacity` 按"最久未使用（LRU）"淘汰；
- **有效期**：命中前检查 TTL，过期条目顺手移除，不再占用容量；
- **常数级开销**：访问序 `LinkedHashMap`，查找/写入/淘汰均摊 O(1)，不会拖过整体时限；
- **并发安全**：所有读写在同一把锁上串行化，任何时刻 `size` 与内容严格一致；
  回退查找只是读缓存，**绝不会因此把下游再打一次**；
- 整体时限截断（TIMEOUT）的读分片同样走同一出口，可回退旧数据。

### 只读状态查询入口

```
GET /api/resilience/status
```

返回每个下游的熔断快照（`circuit`）与回退缓存占用
（`fallbackSize` / `fallbackCapacity` / `fallbackTtlMillis`），便于本地复现与排查。
该入口只读，不改变任何状态；某下游尚未发生可缓存的读成功时，其缓存不创建，占用显示为 0。

## 7. 验证方法

```bash
# 全部 37 个单测（含 Spring 上下文装载）
mvn test

# 按场景单独运行
mvn test -Dtest=CircuitBreakerTest,CircuitHalfOpenTest,CircuitConcurrencyTest  # 熔断/半开/并发
mvn test -Dtest=RetryAndTimeoutTest                                            # 超时/重试/分类
mvn test -Dtest=DuplicateWriteTest                                             # 重复写/幂等去重
mvn test -Dtest=OrchestrationDegradeTest                                       # 降级/部分结果/整体时限
mvn test -Dtest=FallbackCacheTest                                              # 回退缓存容量/过期/并发
mvn test -Dtest=StaleFallbackOrchestrationTest                                 # 旧数据回退/来源标注/写不回退
```

测试日志会打印**每个分片本次是实时、旧数据回退还是静态兜底及判定依据**，例如：

```
[reader] 编排分片结果=降级 来源=旧数据回退 类别=DOWNSTREAM_ERROR 尝试次数=1 数据年龄=5000ms 判定依据=…；回退到 5000ms 前的最近一次成功结果
[writer] 操作类型=NON_IDEMPOTENT_WRITE 不适合回退旧数据（与禁重试判定一致），改用静态兜底
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
curl -s http://localhost:<port>/api/resilience/status
# 各下游熔断状态 + 回退缓存占用，例如：
# profile-service:  CLOSED, fallbackSize=1/64（读成功已缓存，供故障时回退）
# inventory-service: OPEN,  fallbackSize=0/64（从未成功过，只能静态兜底）
# order-write-service: CLOSED, fallbackSize=0（写操作不进回退缓存）
```

### 兼容范围（与之前行为一致的部分）

- 对外接口契约不变：`/api/resilience/aggregate`、`/demo/reset` 语义不变，仅新增 `GET /status`；
- `AggregatedResponse` 的 `complete` / `degraded` / `missing` 语义不变：任何降级分片
  （无论旧数据回退还是静态兜底）仍计为 `present=false` 并出现在 `missing` 中；
- 整体时限语义不变：回退查找为 O(1) 缓存读取，不会把整体响应拖过约定时限；
- 超时/重试/熔断判定不变：旧数据回退只发生在重试与熔断判定完成之后；
- `PartResult` 保留旧构造器，未启用回退缓存时（两参 `Orchestrator` 构造）行为与之前完全一致。

## 8. 测试场景覆盖一览

| 测试类 | 覆盖点 |
| --- | --- |
| `CircuitBreakerTest` | 阈值打开、OPEN 快速失败不阻塞、成功清零、下游隔离 |
| `CircuitHalfOpenTest` | 到期才半开、探测名额受限、探测成功关闭、探测失败重开并重置计时、2 名额组合 |
| `CircuitConcurrencyTest` | 20 线程并发失败恰好打开一次、打开后 100 并发请求全部快速失败无穿透 |
| `RetryAndTimeoutTest` | 连接失败/下游错误/真实超时分类、重试次数准确、非幂等写超时不重试、已确认写不重试 |
| `DuplicateWriteTest` | 迟到确认+重试只写一次、同键重复响应回放、32 并发同键只写一次、无键非幂等不享受保护 |
| `OrchestrationDegradeTest` | 部分结果按时返回、缺失标注与降级值、整体时限截断慢下游、写降级副作用判定一致 |
| `FallbackCacheTest` | 容量上限与 LRU 淘汰、TTL 边界（到期即不可回退）、同键覆盖、计数与内容一致、32 线程并发不突破容量 |
| `StaleFallbackOrchestrationTest` | 读失败回退旧数据并标注新鲜度、从未成功/过期走静态兜底、写操作绝不回退旧数据、整体时限截断也回退、24 并发不打两次下游且失败不入缓存、旧语义不变 |

## 9. 主要代码结构

```
resilience/   FailureCategory / CircuitState / OperationKind / RetryDecision
              ResiliencePolicy        策略值对象
              FailureRules            唯一的"是否重试/副作用是否未知"判定来源
              CircuitBreaker          并发安全状态机（代际许可 + 不可变快照）
              CircuitBreakerRegistry  按下游隔离
              ResilienceClient        超时(Future 限时) + 重试 + 熔断的整合执行
mock/         MockDownstreamServer    脚本编排延迟/故障 + 幂等键去重(每键串行化)
fallback/     FallbackCache           单下游回退缓存（LRU 容量 + TTL，O(1)，锁内一致）
              FallbackCacheRegistry   按下游隔离的回退缓存注册表 + 占用快照
              FallbackPolicy/CachedFallback/FallbackCacheStats  策略/命中结果/占用快照
orchestration/Orchestrator            并发编排 + 整体时限 + 旧数据回退/静态兜底
              PartSource              分片取值来源：LIVE / STALE_FALLBACK / STATIC_FALLBACK
config/       ResilienceConfig/Properties  Bean 装配与配置绑定
web/          DemoController          /api/resilience/aggregate 演示端点
```
