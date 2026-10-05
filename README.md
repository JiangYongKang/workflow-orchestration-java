# 轻量工作流编排（workflow-orchestration-java）

一套可在本地完整验证的轻量工作流编排能力。以**受控的任务执行单元**模拟真实任务的
依赖、临时失败、超时与恢复；提交、查询、恢复的结论**确定、可解释、可复现**。

- 依赖登记与校验（同名任务 / 未知依赖 / 循环依赖，拒绝原因可区分）
- 受控并发（信号量硬上限）+ 有界重试（次数上限 + 单任务耗时上限）
- 失败恢复（从已完成部分继续，已完成任务不重复执行/计数）
- 中途取消（在途任务安全收敛、未启动任务不再开始、已完成结果保留，幂等且为稳定终态）
- 执行历史翻账（逐轮记录结论与每任务尝试/耗时，只追加不覆盖，反复查询一致）
- 提交幂等与冲突区分（原样重复提交返回同一结论；同 id 不同声明明确报 409 冲突；
  非法执行参数在登记前挡掉，不留半成品记录）
- 全程日志打印“触发原因 + 状态流转依据”，快照查询结论稳定

---

## 1. 核心模型与状态流转

### 1.1 工作流状态 `WorkflowState`

```
REGISTERED ──首次执行──▶ RUNNING ──全部任务成功──────────▶ SUCCEEDED（终态）
                           │
                           ├──任一任务重试耗尽/超时收敛──▶ FAILED ──resume──▶ RUNNING …
                           │                                │
                           └──取消请求（任意非成功态）──────┴──▶ CANCELLED（终态，不可恢复）
```

| 状态 | 含义 | 允许的流转 |
|---|---|---|
| `REGISTERED` | 已登记并通过校验，尚未执行 | → `RUNNING` |
| `RUNNING` | 正在调度执行（查询时可见中间任务状态） | → `SUCCEEDED` / `FAILED` / `CANCELLED` |
| `SUCCEEDED` | 全部任务成功的**稳定终态**，不允许再执行 | 终态；再次 `resume`/`cancel` 幂等返回 |
| `FAILED` | 出现稳定失败任务的**稳定终态**（不报告成功） | `resume` → `RUNNING`；`cancel` → `CANCELLED` |
| `CANCELLED` | 被取消的**稳定终态**；恢复请求不能再把它拉起来 | 终态；重复 `cancel`/`resume` 幂等返回 |

### 1.2 任务状态 `TaskState`

```
PENDING ──依赖满足且拿到并发许可──▶ RUNNING ──尝试成功────────────▶ SUCCEEDED（终态）
   │                                  │
   │                                  ├──重试次数达上限仍失败─────▶ FAILED（终态，触发工作流失败）
   │                                  │
   │                                  └──取消请求：当前尝试收敛──▶ CANCELLED（终态）
   │                                     后不再发起新尝试
   ├──工作流已注定失败、任务未及派发──▶ SKIPPED（终态，可在恢复时回到 PENDING）
   │
   └──取消请求、任务未及派发──────────▶ CANCELLED（终态）
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

非法执行参数（`maxConcurrency` / `maxAttempts` / `taskTimeoutMillis` < 1，HTTP `400`）
同样在**登记之前**被挡掉：不存在“提交失败但记录已存在”的半成品状态，
修正取值后同一 id 可正常提交。

---

## 2. 取消语义（中途叫停）

`POST /api/workflows/{id}/cancel` 对工作流发起取消，语义如下：

- **执行中（RUNNING）**：取消标记立即生效——
  - 在途任务**安全收敛**：当前这次尝试仍受单任务超时硬上限约束，尝试结束后
    不再发起新的尝试，任务记为 `CANCELLED`；若尝试在取消前已成功，结果按
    `SUCCEEDED` **保留**；
  - 尚未派发的任务一律 `PENDING → CANCELLED`，**不再开始**；
  - 全部在途任务收敛后，工作流进入 `CANCELLED`。
- **FAILED**：直接流转为 `CANCELLED`；之后的 `resume` 请求幂等返回，**不能**再把它拉起来。
- **SUCCEEDED**：成功终态不可取消，幂等返回既有成功结论。
- **CANCELLED**：重复取消**幂等**，返回同一份结论，不产生额外影响。

取消过程中原有约束全部保持：并发信号量上限不突破、单次尝试耗时硬上限不变、
调度循环有终止判定，**不会无限执行**。取消请求与提交/恢复共用同一把 id 锁，
因此取消返回时工作流必然已到达稳定终态，结论确定、可复现。

---

## 3. 执行历史（事后翻账）

`GET /api/workflows/{id}/history` 返回追加式执行历史：

```json
{
  "workflowId": "demo-2",
  "runs": [
    {"runNumber": 1, "startedAtEpochMillis": 1791177840949,
     "finishedAtEpochMillis": 1791177841051, "conclusion": "FAILED",
     "failureTask": "B", "failureReason": "…",
     "tasks": [{"name": "A", "state": "SUCCEEDED", "attempts": 1,
                "startedAtEpochMillis": …, "finishedAtEpochMillis": …,
                "durationMillis": 0, "message": "succeeded on attempt 1"}]},
    {"runNumber": 2, "…": "…", "conclusion": "SUCCEEDED"}
  ]
}
```

- 每执行一轮（首次提交或一次恢复）**追加**一条 `WorkflowRunRecord`，记录该轮的
  开始/结束时间、结论、失败任务与原因，以及每个任务在**这一轮**的尝试次数、
  起止时间与结果。
- 历史**只追加、不覆盖**：恢复再跑一轮只是多一条记录；反复查询同一份历史，
  内容与顺序完全一致（记录一旦写入即不可变）。
- 恢复轮中保留的已完成任务在该轮记 `attempts=0`、`message="carried over from
  previous run"`，**不重复计数**；其原始尝试数据仍可在它实际执行的那一轮查到。
- 取消本身不产生新的执行轮；被取消工作流的历史保留其取消前各轮的原貌，
  当前状态以快照查询为准。

---

## 4. 提交幂等与冲突语义

同一个 `workflowId` 的提交按**规范化指纹**区分三种情况：

1. **原样重复提交**（指纹一致）：只产生一次执行，返回同一份结论，
   响应标记 `duplicateSubmission=true`。指纹对语义等价的声明做归一：
   任务顺序、依赖集合顺序、`null` 与显式默认值的执行参数差异都不算冲突。
2. **同 id 不同声明**（指纹不一致）：明确报 **HTTP 409**
   `{"error": "WORKFLOW_CONFLICT", …}`，不会把旧结论当成新提交的结论返回；
   既有记录不受任何影响。
3. **非法声明**（结构非法或执行参数 < 1）：在登记前拒绝（422/400），
   不留下任何记录，修正后同 id 可重新提交。

---

## 5. 重试、超时与并发语义

### 5.1 有界重试（绝不无限执行）

- 尝试次数从 1 开始计数，`maxAttempts` 为**每个执行轮内**的上限（默认 3）。
- 单次尝试失败（业务失败或超时）后：
  - 未达上限：退避 `retry-backoff-millis` 后重试；
  - 达到上限：任务进入稳定 `FAILED`，工作流进入稳定 `FAILED`。
- 恢复属于**新的执行轮**，被重置任务的尝试计数从零重新开始；已完成任务不参与。

### 5.2 单任务耗时硬上限

- 每次尝试由看门狗（`Future.get(timeout)`）施加 `taskTimeoutMillis` 硬上限。
- 超时即**中断**任务线程，把该次尝试记为一次失败（原因含 `attempt timed out after …ms`）。
- 因此无论任务如何阻塞，单任务总耗时与总尝试次数都有确定上界：
  `总耗时 ≤ maxAttempts × taskTimeoutMillis + 退避`。

### 5.3 受控并发（绝不超过上限）

- 每个工作流持有一把信号量，许可数 = `maxConcurrency`。
- 调度器只在 **`tryAcquire` 成功后**才把任务从 `PENDING` 派发出去；
  运行中任务数永远 ≤ 上限（快照中的 `peakConcurrency` 记录实测峰值）。
- 互不依赖的任务可并发；有依赖的任务严格按依赖先后执行。
- 不同工作流之间互不影响；调度线程池大小由 `workflow.engine-threads` 控制。

---

## 6. 受控任务行为（确定性模拟）

任务的 `behavior` 字段选择行为，`params` 为参数（均为字符串）：

| behavior | params | 语义 |
|---|---|---|
| `ALWAYS_SUCCEED` | `[耗时ms]?` | 恒成功；可指定模拟耗时 |
| `ALWAYS_FAIL` | `[耗时ms]?` | 恒注入临时错误，用于演示重试耗尽 |
| `FAIL_FIRST_N` | `[N, 耗时ms?]` | 单轮内前 N 次尝试失败、第 N+1 次起成功 |
| `TIMEOUT` | — | 长时间阻塞，由看门狗判定超时 |
| `FAIL_TOTAL_N` | `[N, 耗时ms?]` | **跨恢复轮**总共失败 N 次后成功，用于确定性演示失败恢复 |

---

## 7. 可配置项

`src/main/resources/application.properties` 中的全局默认值，可被单个工作流提交体中的
同名字段覆盖（`maxConcurrency` / `maxAttempts` / `taskTimeoutMillis`）：

| 配置键 | 默认 | 含义 |
|---|---|---|
| `workflow.max-concurrency` | 4 | 单个工作流的并发任务数硬上限 |
| `workflow.max-attempts` | 3 | 每个执行轮内单任务的最大尝试次数 |
| `workflow.task-timeout-millis` | 5000 | 单次尝试的超时硬上限 |
| `workflow.retry-backoff-millis` | 100 | 两次尝试之间的退避 |
| `workflow.engine-threads` | 16 | 工作流调度线程池大小 |

非法覆盖值（< 1）会在**登记之前**被拒绝（HTTP `400`），不留下任何执行记录。

---

## 8. REST 接口

| 方法与路径 | 说明 |
|---|---|
| `PUT /api/workflows` | 提交并同步执行；原样重复/并发提交幂等返回同一份记录（`duplicateSubmission=true`）；同 id 不同声明返回 `409` |
| `GET /api/workflows/{id}` | 查询不可变状态快照；不存在返回 `404` |
| `POST /api/workflows/{id}/resume` | 失败恢复；非 `FAILED` 状态（含 `CANCELLED`）幂等返回当前快照 |
| `POST /api/workflows/{id}/cancel` | 取消；幂等，`CANCELLED` 为稳定终态；不存在返回 `404` |
| `GET /api/workflows/{id}/history` | 查询追加式执行历史（逐轮记录）；不存在返回 `404` |

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

冲突响应示例（同 id 不同声明，HTTP 409）：

```json
{"error": "WORKFLOW_CONFLICT",
 "message": "workflow id 'demo-1' already registered with a different definition"}
```

### 8.1 兼容范围

- 既有接口（提交/查询/恢复）的 URL、请求体与既有字段语义**完全不变**；
  原样重复提交依旧幂等返回同一份结论。
- 新增：`cancel`/`history` 两个端点、快照中可能出现的 `CANCELLED` 状态、
  以及同 id 不同声明时的 `409` 响应（此前会错误地返回旧记录——属于语义修正）。
- 历史与取消能力为纯增量：不取消、不查历史的调用方行为与之前一致。

---

## 9. 本地验证方法

环境：JDK 21+、Maven 3.9+。

### 9.1 运行全部测试（推荐，无需外部依赖）

```bash
mvn test
```

共 27 个用例，覆盖：

| 测试类 | 覆盖场景 |
|---|---|
| `WorkflowValidatorTest` | 同名任务、未知依赖、循环依赖、自依赖被拒（原因可区分）；合法 DAG 通过 |
| `WorkflowRetryTest` | 重试耗尽（尝试次数恰为上限、下游 SKIPPED）；临时错误重试成功；超时硬上限 |
| `WorkflowConcurrencyTest` | 并发上限=2（峰值恰好 2、时间窗重叠）；上限=1 严格串行 |
| `WorkflowDependencyOrderTest` | 菱形依赖执行顺序满足依赖，互不依赖任务并发 |
| `WorkflowRecoveryTest` | 失败后恢复成功（A 不重跑/不重复计数）；恢复仍失败时结论稳定 |
| `WorkflowIdempotencyTest` | 顺序重复提交、8 线程并发提交只产生一次执行；非法提交可反复拒绝且不留记录 |
| `WorkflowCancelTest` | 执行中取消（在途收敛结果保留、未启动不再开始）、取消幂等、FAILED/SUCCEEDED 上取消、取消后恢复无效 |
| `WorkflowHistoryTest` | 两轮历史只追加不覆盖、逐任务尝试计数、反复查询一致、取消不产生新轮次 |
| `WorkflowSubmissionConflictTest` | 同 id 不同声明报冲突、语义等价声明仍幂等、非法参数登记前挡掉且可修正重提 |
| `WorkflowApiSmokeTest` | 真实 HTTP：422/400/409/404、成功、查询、幂等、失败→恢复→成功、取消→恢复无效、历史翻账 |

日志中 `[测试断言]` / `[HTTP 测试]` 行会打印**触发原因与状态流转依据**，便于人工核对。
例如重试耗尽时可见：

```
任务 flaky 状态流转 RUNNING -> FAILED，依据=重试次数已达上限3，最后原因=injected transient error by ALWAYS_FAIL
工作流状态流转 RUNNING -> FAILED，依据=任务 flaky 进入稳定失败（…），峰值并发=1
```

### 9.2 启动服务并用 curl 验证

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

# 5) 翻账：demo-2 的两轮历史（第1轮 FAILED、第2轮 SUCCEEDED，逐任务尝试次数）
curl -s localhost:8080/api/workflows/demo-2/history

# 6) 取消：慢任务执行中发起取消（另开终端先提交，再立即取消）
curl -s -X PUT localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"demo-3","maxConcurrency":2,"maxAttempts":3,
  "tasks":[
    {"name":"A","dependsOn":[],"behavior":"ALWAYS_SUCCEED","params":["30000"]},
    {"name":"B","dependsOn":["A"],"behavior":"ALWAYS_SUCCEED","params":[]}]}' &
sleep 1
curl -s -X POST localhost:8080/api/workflows/demo-3/cancel   # CANCELLED；B 不再开始
curl -s -X POST localhost:8080/api/workflows/demo-3/cancel   # 重复取消：幂等
curl -s -X POST localhost:8080/api/workflows/demo-3/resume   # 取消后恢复无效：仍 CANCELLED

# 7) 提交冲突：同 id 不同声明 -> 409 WORKFLOW_CONFLICT
curl -s -X PUT localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"demo-1","tasks":[
    {"name":"A","dependsOn":[],"behavior":"ALWAYS_FAIL","params":[]}]}'

# 8) 非法执行参数：登记前挡掉（400），随后查询仍 404，修正后可正常提交
curl -s -X PUT localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"demo-4","maxConcurrency":0,"tasks":[
    {"name":"A","dependsOn":[],"behavior":"ALWAYS_SUCCEED","params":[]}]}'
curl -s localhost:8080/api/workflows/demo-4   # 404：未留下任何记录
```

### 9.3 复现性说明

所有任务行为均为确定性逻辑（无随机数、无真实 IO），时间相关用例只断言
有界区间与状态/计数，不对墙钟时间做精确断言，因此多次运行结论一致。
