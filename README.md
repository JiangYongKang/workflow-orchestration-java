# 轻量工作流编排（workflow-orchestration-java）

一套可在本地完整验证的轻量工作流编排能力。以**受控的任务执行单元**模拟真实任务的
依赖、临时失败、超时与恢复；提交、查询、恢复的结论**确定、可解释、可复现**。

- 依赖登记与校验（同名任务 / 未知依赖 / 循环依赖，拒绝原因可区分）
- 受控并发（信号量硬上限）+ 有界重试（次数上限 + 单任务耗时上限）
- 失败恢复（从已完成部分继续，已完成任务不重复执行/计数）
- 提交幂等（顺序与并发重复提交只产生一份执行记录）
- 全程日志打印“触发原因 + 状态流转依据”，快照查询结论稳定

---

## 1. 核心模型与状态流转

### 1.1 工作流状态 `WorkflowState`

```
REGISTERED ──首次执行──▶ RUNNING ──全部任务成功──────────▶ SUCCEEDED（终态）
                           │
                           └──任一任务重试耗尽/超时收敛──▶ FAILED ──resume──▶ RUNNING …
```

| 状态 | 含义 | 允许的流转 |
|---|---|---|
| `REGISTERED` | 已登记并通过校验，尚未执行 | → `RUNNING` |
| `RUNNING` | 正在调度执行（查询时可见中间任务状态） | → `SUCCEEDED` / `FAILED` |
| `SUCCEEDED` | 全部任务成功的**稳定终态**，不允许再执行 | 终态；再次 `resume` 幂等返回 |
| `FAILED` | 出现稳定失败任务的**稳定终态**（不报告成功） | `resume` → `RUNNING` |

### 1.2 任务状态 `TaskState`

```
PENDING ──依赖满足且拿到并发许可──▶ RUNNING ──尝试成功────────────▶ SUCCEEDED（终态）
   │                                  │
   │                                  └──重试次数达上限仍失败─────▶ FAILED（终态，触发工作流失败）
   │
   └──工作流已注定失败、任务未及派发──▶ SKIPPED（终态，可在恢复时回到 PENDING）
```

- 任务仅在其**全部依赖均为 `SUCCEEDED`** 时才可能被派发，因此实际执行顺序必然满足依赖关系。
- 任一任务 `FAILED` 后：在途任务允许跑完；所有未派发任务确定性地 `SKIPPED`；
  工作流进入 `FAILED`，并记录 `failureTask` 与可解释的 `failureReason`。
- **恢复时**：`SUCCEEDED` 任务原样保留（含尝试计数与耗时，绝不重跑）；
  `FAILED` 任务回到 `PENDING` 且尝试计数清零；被其连累的 `SKIPPED` 任务按依赖闭包
  重新解锁为 `PENDING`，随后正常调度。

### 1.3 登记校验拒绝原因 `RejectReason`

按**固定顺序**判定，结论稳定、可复现：

1. `DUPLICATE_TASK`：同名任务重复声明
2. `UNKNOWN_DEPENDENCY`：依赖了未声明的任务（含在依赖列表中引用不存在的名字）
3. `CYCLIC_DEPENDENCY`：依赖图存在环（含自依赖）

非法定义在登记阶段即被拒绝（HTTP `422`，响应体含 `reason`），**不会留下任何执行记录**，
可修改后再次提交。

---

## 2. 重试、超时与并发语义

### 2.1 有界重试（绝不无限执行）

- 尝试次数从 1 开始计数，`maxAttempts` 为**每个执行轮内**的上限（默认 3）。
- 单次尝试失败（业务失败或超时）后：
  - 未达上限：退避 `retry-backoff-millis` 后重试；
  - 达到上限：任务进入稳定 `FAILED`，工作流进入稳定 `FAILED`。
- 恢复属于**新的执行轮**，被重置任务的尝试计数从零重新开始；已完成任务不参与。

### 2.2 单任务耗时硬上限

- 每次尝试由看门狗（`Future.get(timeout)`）施加 `taskTimeoutMillis` 硬上限。
- 超时即**中断**任务线程，把该次尝试记为一次失败（原因含 `attempt timed out after …ms`）。
- 因此无论任务如何阻塞，单任务总耗时与总尝试次数都有确定上界：
  `总耗时 ≤ maxAttempts × taskTimeoutMillis + 退避`。

### 2.3 受控并发（绝不超过上限）

- 每个工作流持有一把信号量，许可数 = `maxConcurrency`。
- 调度器只在 **`tryAcquire` 成功后**才把任务从 `PENDING` 派发出去；
  运行中任务数永远 ≤ 上限（快照中的 `peakConcurrency` 记录实测峰值）。
- 互不依赖的任务可并发；有依赖的任务严格按依赖先后执行。
- 不同工作流之间互不影响；调度线程池大小由 `workflow.engine-threads` 控制。

---

## 3. 受控任务行为（确定性模拟）

任务的 `behavior` 字段选择行为，`params` 为参数（均为字符串）：

| behavior | params | 语义 |
|---|---|---|
| `ALWAYS_SUCCEED` | `[耗时ms]?` | 恒成功；可指定模拟耗时 |
| `ALWAYS_FAIL` | `[耗时ms]?` | 恒注入临时错误，用于演示重试耗尽 |
| `FAIL_FIRST_N` | `[N, 耗时ms?]` | 单轮内前 N 次尝试失败、第 N+1 次起成功 |
| `TIMEOUT` | — | 长时间阻塞，由看门狗判定超时 |
| `FAIL_TOTAL_N` | `[N, 耗时ms?]` | **跨恢复轮**总共失败 N 次后成功，用于确定性演示失败恢复 |

---

## 4. 可配置项

`src/main/resources/application.properties` 中的全局默认值，可被单个工作流提交体中的
同名字段覆盖（`maxConcurrency` / `maxAttempts` / `taskTimeoutMillis`）：

| 配置键 | 默认 | 含义 |
|---|---|---|
| `workflow.max-concurrency` | 4 | 单个工作流的并发任务数硬上限 |
| `workflow.max-attempts` | 3 | 每个执行轮内单任务的最大尝试次数 |
| `workflow.task-timeout-millis` | 5000 | 单次尝试的超时硬上限 |
| `workflow.retry-backoff-millis` | 100 | 两次尝试之间的退避 |
| `workflow.engine-threads` | 16 | 工作流调度线程池大小 |

非法覆盖值（< 1）会被拒绝（HTTP `400`）。

---

## 5. REST 接口

| 方法与路径 | 说明 |
|---|---|
| `PUT /api/workflows` | 提交并同步执行；重复/并发提交幂等返回同一份记录（`duplicateSubmission=true`） |
| `GET /api/workflows/{id}` | 查询不可变状态快照；不存在返回 `404` |
| `POST /api/workflows/{id}/resume` | 失败恢复；非 `FAILED` 状态幂等返回当前快照 |

提交体示例：

```json
{
  "workflowId": "demo-1",
  "maxConcurrency": 2,
  "maxAttempts": 3,
  "taskTimeoutMillis": 5000,
  "tasks": [
    {"name": "A", "dependsOn": [],                "behavior": "ALWAYS_SUCCEED", "params": []},
    {"name": "B", "dependsOn": ["A"],             "behavior": "FAIL_FIRST_N",   "params": ["2", "0"]},
    {"name": "C", "dependsOn": ["A"],             "behavior": "ALWAYS_SUCCEED", "params": []},
    {"name": "D", "dependsOn": ["B", "C"],        "behavior": "ALWAYS_SUCCEED", "params": []}
  ]
}
```

错误响应示例（循环/未知依赖等，HTTP 422）：

```json
{"error": "VALIDATION_REJECTED", "reason": "UNKNOWN_DEPENDENCY",
 "message": "task 'A' depends on unknown task: GHOST"}
```

---

## 6. 本地验证方法

环境：JDK 21+、Maven 3.9+。

### 6.1 运行全部测试（推荐，无需外部依赖）

```bash
mvn test
```

共 19 个用例，覆盖：

| 测试类 | 覆盖场景 |
|---|---|
| `WorkflowValidatorTest` | 同名任务、未知依赖、循环依赖、自依赖被拒（原因可区分）；合法 DAG 通过 |
| `WorkflowRetryTest` | 重试耗尽（尝试次数恰为上限、下游 SKIPPED）；临时错误重试成功；超时硬上限 |
| `WorkflowConcurrencyTest` | 并发上限=2（峰值恰好 2、时间窗重叠）；上限=1 严格串行 |
| `WorkflowDependencyOrderTest` | 菱形依赖执行顺序满足依赖，互不依赖任务并发 |
| `WorkflowRecoveryTest` | 失败后恢复成功（A 不重跑/不重复计数）；恢复仍失败时结论稳定 |
| `WorkflowIdempotencyTest` | 顺序重复提交、8 线程并发提交只产生一次执行；非法提交可反复拒绝且不留记录 |
| `WorkflowApiSmokeTest` | 真实 HTTP：422 拒绝、成功、查询、幂等、404、失败→恢复→成功全链路 |

日志中 `[测试断言]` / `[HTTP 测试]` 行会打印**触发原因与状态流转依据**，便于人工核对。
例如重试耗尽时可见：

```
任务 flaky 状态流转 RUNNING -> FAILED，依据=重试次数已达上限3，最后原因=injected transient error by ALWAYS_FAIL
工作流状态流转 RUNNING -> FAILED，依据=任务 flaky 进入稳定失败（…），峰值并发=1
```

### 6.2 启动服务并用 curl 验证

```bash
mvn spring-boot:run
```

另开终端：

```bash
# 1) 合法工作流（菱形依赖 + 前2次失败后成功），最终 SUCCEEDED
curl -s -X PUT localhost:8080/api/workflows \
  -H 'Content-Type: application/json' -d '{
    "workflowId":"demo-1","maxConcurrency":2,"maxAttempts":3,
    "tasks":[
      {"name":"A","dependsOn":[],"behavior":"ALWAYS_SUCCEED","params":["50"]},
      {"name":"B","dependsOn":["A"],"behavior":"FAIL_FIRST_N","params":["2","0"]},
      {"name":"C","dependsOn":["A"],"behavior":"ALWAYS_SUCCEED","params":["50"]},
      {"name":"D","dependsOn":["B","C"],"behavior":"ALWAYS_SUCCEED","params":[]}
    ]}'

# 2) 循环依赖被拒（注意 reason=CYCLIC_DEPENDENCY）
curl -s -X PUT localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"bad","tasks":[
    {"name":"A","dependsOn":["B"],"behavior":"ALWAYS_SUCCEED","params":[]},
    {"name":"B","dependsOn":["A"],"behavior":"ALWAYS_SUCCEED","params":[]}]}'

# 3) 查询
curl -s localhost:8080/api/workflows/demo-1

# 4) 失败 → 恢复 → 成功（B 跨轮共失败 2 次，首轮上限2恰好失败，恢复后成功）
curl -s -X PUT localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"demo-2","maxConcurrency":4,"maxAttempts":2,
  "tasks":[
    {"name":"A","dependsOn":[],"behavior":"ALWAYS_SUCCEED","params":[]},
    {"name":"B","dependsOn":[],"behavior":"FAIL_TOTAL_N","params":["2","0"]},
    {"name":"C","dependsOn":["B"],"behavior":"ALWAYS_SUCCEED","params":[]}]}'
curl -s -X POST localhost:8080/api/workflows/demo-2/resume
```

### 6.3 复现性说明

所有任务行为均为确定性逻辑（无随机数、无真实 IO），时间相关用例只断言
有界区间与状态/计数，不对墙钟时间做精确断言，因此多次运行结论一致。
