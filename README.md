# 多下游调用编排与弹性能力（downstream-call-resilience-java bootstrap-c）

在 Spring Boot 工程内实现一套面向多个下游依赖的调用编排能力，并用**纯本地、可复现**的方式模拟下游的
延迟、连接失败、错误响应、写确认与重复响应。各类失败的返回结果与熔断状态变化均可预期、可解释，
且不改动既有对外接口契约（仅新增 `/api/resilience/**` 路径）。

> **本轮（bootstrap-c）新增：读失败的"最近一次成功结果"回退（旧数据）。**
> 降级不再只是写死的占位值：读下游失败时优先回退它最近一次成功的结果，并在分片里标明
> 这是旧数据、来源与新鲜度；只有从未成功过、旧数据过期，或操作不适合回退旧数据（写操作）时，
> 才退回到原来的静态缺省值。另提供只读状态入口查看各下游熔断状态与回退缓存占用。

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
| **读失败优先回退最近一次成功旧数据（含来源/新鲜度）** | `fallback/FallbackCache`、`FallbackCacheRegistry`、`Orchestrator` |
| **旧数据容量上限 + 有效期 + LRU，查找/淘汰摊还 O(1)** | `fallback/FallbackCache` |
| **只读查看健康/熔断状态与回退缓存占用** | `status/ResilienceStatusService`、`GET /api/resilience/status` |
| 本地模拟延迟/故障/重复响应 | `mock/MockDownstreamServer`（脚本行为 + 每键串行化去重表） |

## 1.1 降级数据来源（新增）

每个分片 `PartResult` 新增两个字段（**只增不改**，原字段与语义不变）：

| `source` | 含义 | `staleAgeMillis` |
| --- | --- | --- |
| `LIVE` | 本次实时从下游取得的成功结果（`present=true`） | `-1` |
| `STALE` | 读失败后回退的该下游**最近一次成功**结果（旧数据，`present=false` 仍算降级/缺失） | 旧数据年龄（ms） |
| `STATIC_DEFAULT` | 无可用旧数据时的静态缺省值（**原有降级行为**） | `-1` |

回退选择规则（`Orchestrator#degrade`）：

1. 只有**读操作（READ）的成功结果**才会写入回退缓存；**失败结果绝不缓存，写结果绝不缓存**。
2. 读操作失败（含整体时限被截断的慢读）→ 先查同请求键的有效旧数据：命中则 `STALE`（带年龄）。
3. 从未成功过、旧数据已过 TTL、或该操作不是读 → 用静态缺省值 `STATIC_DEFAULT`。
4. **写操作（`IDEMPOTENT_WRITE` / `NON_IDEMPOTENT_WRITE`）一律不回退旧结果。**
   尤其非幂等写超时/下游错误、副作用是否生效未知时，只能静态兜底并保持降级标记——
   这与 `FailureRules` 的"禁止重试"结论共用同一判定，不会出现"一边不敢重发、一边拿旧结果当成功"。

## 1.2 回退缓存的容量与淘汰（新增）

每个下游一个独立 `FallbackCache`，容量与 TTL 取自该下游策略：

- **有效期**：`age > ttl` 即过期，过期条目绝不再作为有效数据回退（`age == ttl` 边界仍有效）。
- **容量淘汰（LRU）**：清理过期后条数仍达上限时，淘汰"最近最少访问"的一条（命中刷新访问序）。
- **复杂度**：访问序 `LinkedHashMap` 保证 LRU 淘汰取最旧条目；配合不可变插入标记队列做惰性过期清理，
  查找 / 写入 / 过期淘汰均为**摊还 O(1)**，回退是纯内存查找，不会再对下游发起调用。
- **并发安全**：所有变更在同一监视器上串行化，map 内容与 `size()` 计数永远一致，
  并发写入不会超额、不会丢计数、也不会把失败结果缓存下来。
- **下游隔离**：缓存在 `FallbackCacheRegistry` 中按下游名分存，互不混淆、互不淘汰。

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
resilience.downstreams.<下游名>.fallback-capacity=16      # 回退旧数据条数上限（LRU）
resilience.downstreams.<下游名>.fallback-ttl-millis=30000 # 回退旧数据有效期（ms），过期不再回退
```

缺省值：容量 16 条、TTL 30s（`ResiliencePolicy#DEFAULT_FALLBACK_*`）。
写下游可照常配置容量/TTL（会显示在只读状态里），但其成功结果不会写入、失败也不会回退旧数据。

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
2. 整体时限到期时，未完成分片直接按 `TIMEOUT` 降级，响应仍在时限内返回（慢读此时也优先 STALE）；
3. 每个失败分片回填降级值：读分片优先 STALE（带年龄），写分片/无有效旧数据时 STATIC_DEFAULT，
   `PartResult` 标注 `present/degraded/category/attempts/reason` 以及新增的 `source/staleAgeMillis`；
4. `AggregatedResponse` 汇总 `complete`、`missing`（含下游名、类别、**来源**与原因）与耗时。

**与原有行为的兼容范围（哪些完全没变）：**

- `complete` / `degraded` / `missing` 语义不变：STALE 仍是降级、仍计入 `missing`（缺的是实时结果）；
  只有 `LIVE` 才算 `present=true`。
- 整体时限要求、各分片并发模型、超时/重试/熔断策略与判定全部不变。
- `PartResult` 为"只增字段"：原 7 个构造参数顺序与含义保留（另提供 `live/stale/staticDefault` 工厂）；
  对外仍是 `GET /api/resilience/aggregate`，只是结果里多了 `source`、`staleAgeMillis` 两个字段。
- 写操作降级语义与之前一致（静态兜底 + 副作用判定），不会因为新缓存而返回旧的写结果。

## 6. 只读状态查询（新增）

`GET /api/resilience/status` 返回每个下游的只读快照（不触发下游调用、不改运行状态）：

```json
{
  "reportedAtMillis": 1791325304200,
  "downstreams": [
    {
      "downstreamName": "inventory-service",
      "circuitState": "CLOSED",
      "consecutiveFailures": 0,
      "circuitRecoverInMillis": 0,
      "operationKind": "READ",
      "fallbackSize": 1,
      "fallbackCapacity": 4,
      "fallbackTtlMillis": 30000,
      "oldestEntryAgeMillis": 10893
    }
  ]
}
```

- `circuitState`：CLOSED / OPEN / HALF_OPEN；OPEN 时 `circuitRecoverInMillis` 为距半开探测的剩余毫秒。
- `fallbackSize/capacity/ttlMillis/oldestEntryAgeMillis`：回退缓存有效占用、上限、有效期与最旧条目年龄。

## 7. 验证方法

```bash
# 全部单测（含 Spring 上下文装载）
mvn test

# 按场景单独运行
mvn test -Dtest=CircuitBreakerTest,CircuitHalfOpenTest,CircuitConcurrencyTest  # 熔断/半开/并发
mvn test -Dtest=RetryAndTimeoutTest                                            # 超时/重试/分类
mvn test -Dtest=DuplicateWriteTest                                             # 重复写/幂等去重
mvn test -Dtest=OrchestrationDegradeTest                                       # 降级/部分结果/整体时限
mvn test -Dtest=FallbackCacheTest                                              # 旧数据缓存：容量/TTL/LRU/并发
mvn test -Dtest=FallbackOrchestrationTest                                      # 旧数据回退/写不回退/状态
mvn test -Dtest=FallbackConcurrencyTest                                        # 并发：容量/计数/不补打下游
```

测试日志（控制台 + `target/surefire-reports`）会打印**每个下游本次是 LIVE / STALE / STATIC_DEFAULT、
判定依据与新鲜度**，例如：

```
[inventory-service] 编排分片结果=降级(旧数据可用) 来源=STALE(旧数据回退) 类别=DOWNSTREAM_ERROR
  尝试次数=3 新鲜度=88ms前 判定依据=已达最大尝试次数 3，停止重试；... 旧值=inventory-snapshot-v1
[writer] 编排分片结果=降级缺失 来源=STATIC_DEFAULT(静态兜底) 类别=TIMEOUT
  判定依据=写操作不回退旧数据（写副作用状态未知，禁止以旧结果冒充成功） 降级值=write-uncertain
```

端到端演示：

```bash
mvn spring-boot:run
curl -s http://localhost:<port>/api/resilience/aggregate
# 第 1 次：inventory-service 实时成功(LIVE)，其结果作为旧数据缓存；
# 随后请求：inventory 持续故障，先回退旧数据 source=STALE(staleAgeMillis 递增)；
# 30s（TTL）后：旧数据过期，退化为 source=STATIC_DEFAULT（静态缺省值）；期间熔断器在
# CLOSED/OPEN/HALF_OPEN 间迁移，另外两个下游始终完整。
# order-write-service 携带幂等键 idem-demo-001，重复调用返回 duplicate=true 且写只执行一次，
# 写失败时只走静态兜底，绝不回退旧写结果。
curl -s http://localhost:<port>/api/resilience/status               # 只读：熔断状态 + 回退缓存占用
curl -s -X POST http://localhost:<port>/api/resilience/demo/reset   # 重置模拟脚本与回退缓存
```

## 8. 测试场景覆盖一览

| 测试类 | 覆盖点 |
| --- | --- |
| `CircuitBreakerTest` | 阈值打开、OPEN 快速失败不阻塞、成功清零、下游隔离 |
| `CircuitHalfOpenTest` | 到期才半开、探测名额受限、探测成功关闭、探测失败重开并重置计时、2 名额组合 |
| `CircuitConcurrencyTest` | 20 线程并发失败恰好打开一次、打开后 100 并发请求全部快速失败无穿透 |
| `RetryAndTimeoutTest` | 连接失败/下游错误/真实超时分类、重试次数准确、非幂等写超时不重试、已确认写不重试 |
| `DuplicateWriteTest` | 迟到确认+重试只写一次、同键重复响应回放、32 并发同键只写一次、无键非幂等不享受保护 |
| `OrchestrationDegradeTest` | 部分结果按时返回、缺失标注与降级值、整体时限截断慢下游、写降级副作用判定一致 |
| `FallbackCacheTest` | 旧数据存取与年龄、TTL 边界与过期不回退、容量 LRU、覆盖刷新不阻塞过期、并发不超额且内容与计数一致、同键并发覆盖 |
| `FallbackOrchestrationTest` | STALE 优先及年龄标注、从未成功→静态兜底、过期→静态兜底、非幂等写/幂等写不回退旧数据、恢复后刷新旧数据、慢读整体时限截断走 STALE、只读状态 |
| `FallbackConcurrencyTest` | 600 次并发聚合容量/计数一致、失败不缓存、回退纯内存不补打下游、32 并发失败共用同一条旧数据 |

## 9. 主要代码结构

```
resilience/   FailureCategory / CircuitState / OperationKind / RetryDecision
              ResiliencePolicy        策略值对象（含 fallbackCapacity / fallbackTtl）
              FailureRules            唯一的"是否重试/副作用是否未知"判定来源
              CircuitBreaker          并发安全状态机（代际许可 + 不可变快照 + 只读状态查询）
              CircuitBreakerRegistry  按下游隔离
              ResilienceClient        超时(Future 限时) + 重试 + 熔断的整合执行
fallback/     DataSource              LIVE / STALE / STATIC_DEFAULT
              FallbackEntry           一条最近成功结果（键/值/捕获时间）
              FallbackCache           容量+TTL+LRU 的并发安全回退缓存（摊还 O(1)）
              FallbackCacheRegistry   按下游隔离，容量/TTL 取自各下游策略
status/       DownstreamStatus/ResilienceStatus   只读状态快照
              ResilienceStatusService  聚合熔断状态与回退缓存占用
mock/         MockDownstreamServer    脚本编排延迟/故障 + 幂等键去重(每键串行化)
orchestration/Orchestrator            并发编排 + 整体时限 + 旧数据/静态两级降级
              PartResult              新增 source / staleAgeMillis（只增不改）
config/       ResilienceConfig/Properties  Bean 装配与配置绑定
web/          DemoController          /aggregate 演示、/status 只读状态、/demo/reset 重置
```
