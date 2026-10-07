# 轻量工作流编排（workflow-orchestration-java）

一套可在本地完整验证的轻量工作流编排能力。以**受控的任务执行单元**模拟真实任务的
依赖、临时失败、超时与恢复；提交、查询、恢复的结论**确定、可解释、可复现**。

- 依赖登记与校验（同名任务 / 未知依赖 / 循环依赖，拒绝原因可区分）
- 受控并发（信号量硬上限）+ 有界重试（次数上限 + 单任务耗时上限）
- 失败恢复（从已完成部分继续，已完成任务不重复执行/计数）
- 中途取消（在途任务安全收敛、未启动不再开始、已完成结果保留；幂等且为稳定终态）
- 执行历史留痕（逐轮起止/结论 + 每任务尝试次数/耗时/结果，只增不改）
- 提交幂等 + 冲突检测（同 id 同声明幂等；同 id 不同声明显式 409；非法取值登记前拦截）
- 全程日志打印“触发原因 + 状态流转依据”，快照查询结论稳定

---

## 1. 核心模型与状态流转

### 1.1 工作流状态 `WorkflowState`

```
REGISTERED ──首次执行──▶ RUNNING ──全部任务成功──────────▶ SUCCEEDED（终态）
   │                     │
   │                     ├──任一任务重试耗尽/超时收敛──▶ FAILED ──resume──▶ RUNNING …
   │                     │                              │
   └──cancel─────────────┴──────────cancel──────────────┴──▶ CANCELLED（终态，不可逆）
```

| 状态 | 含义 | 允许的流转 |
|---|---|---|
| `REGISTERED` | 已登记并通过校验，尚未执行 | → `RUNNING`；`cancel` → `CANCELLED` |
| `RUNNING` | 正在调度执行（查询时可见中间任务状态） | → `SUCCEEDED` / `FAILED` / `CANCELLED` |
| `SUCCEEDED` | 全部任务成功的**稳定终态**，不允许再执行 | 终态；`resume`/`cancel` 均幂等返回、不改变结论 |
| `FAILED` | 出现稳定失败任务的**稳定终态**（不报告成功） | `resume` → `RUNNING`；`cancel` → `CANCELLED` |
| `CANCELLED` | 被取消的**稳定终态**：恢复请求不会把它重新拉起 | 终态；任何 `resume`/`cancel` 幂等返回 |

### 1.2 任务状态 `TaskState`

```
PENDING ──依赖满足且拿到并发许可──▶ RUNNING ──尝试成功────────────▶ SUCCEEDED（终态）
   │                                  │
   │                                  ├──重试次数达上限仍失败─────▶ FAILED（终态，触发工作流失败）
   │                                  │
   │                                  └──取消时已跑尝试收敛──────▶ CANCELLED（终态，保留尝试计数）
   │
   ├──工作流已注定失败、任务未及派发──▶ SKIPPED（终态，可在恢复时回到 PENDING）
   │
   └──工作流被取消、任务未及派发────▶ CANCELLED（终态，不进入恢复重置）
```

- 任务仅在其**全部依赖均为 `SUCCEEDED`** 时才可能被派发，因此实际执行顺序必然满足依赖关系。
- 任一任务 `FAILED` 后：在途任务允许跑完；所有未派发任务确定性地 `SKIPPED`；
  工作流进入 `FAILED`，并记录 `failureTask` 与可解释的 `failureReason`。
- **恢复时**：`SUCCEEDED` 任务原样保留（含尝试计数与耗时，绝不重跑）；
  `FAILED` 任务回到 `PENDING` 且尝试计数清零；被其连累的 `SKIPPED` 任务按依赖闭包
  重新解锁为 `PENDING`，随后正常调度。
  注意：这里说的是**当前状态快照**中的累计计数；逐轮历史留痕（见第 4 节）中，
  保留未重跑的任务在新一轮记录里记 0 次尝试、起止与耗时均为 0。

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

## 3. 取消语义（`POST /api/workflows/{id}/cancel`）

- **随时可发起**：取消不占用提交/恢复的 id 锁，执行中即可送达。
- **安全收敛**：收到取消后，调度器立即停止派发新任务；在途任务当前这次尝试
  允许跑完（仍受单任务超时硬上限约束），之后不再重试，落到 `CANCELLED`。
- **结果保留**：已 `SUCCEEDED` 的任务结果与尝试计数原样保留；未启动的任务
  置为 `CANCELLED`（区别于失败连累的 `SKIPPED`），尝试计数为 0。
- **约束不变**：取消过程中并发上限、单任务耗时上界、尝试次数上界全部继续成立；
  取消等待本身也有界（由 `maxAttempts × (taskTimeoutMillis + backoff)` 推导上界）。
- **幂等**：重复取消、对 `SUCCEEDED` 工作流的取消都是幂等空操作，返回当前快照。
- **稳定终态**：`CANCELLED` 不可逆——之后的 `resume` 被幂等忽略，不会重新拉起；
  对 `FAILED` 工作流取消会直接流转 `FAILED -> CANCELLED`，封死后续恢复。

---

## 4. 执行历史留痕

快照中的 `history` 字段是按轮次只增不改的留痕数组，每轮一条：

```json
{
  "runNumber": 2,
  "startedAtEpochMillis": 1791158974389,
  "finishedAtEpochMillis": 1791158974391,
  "outcome": "SUCCEEDED",
  "failureTask": null,
  "failureReason": "",
  "tasks": [
    {"name": "A", "finalState": "SUCCEEDED", "attempts": 0,
     "startedAtEpochMillis": 0, "finishedAtEpochMillis": 0,
     "durationMillis": 0, "message": "succeeded on attempt 1"},
    {"name": "B", "finalState": "SUCCEEDED", "attempts": 1,
     "startedAtEpochMillis": 1791158974389, "finishedAtEpochMillis": 1791158974390,
     "durationMillis": 1, "message": "succeeded on attempt 1"}
  ]
}
```

- 每轮记录：轮次号、起止时间、最终结论（`SUCCEEDED`/`FAILED`/`CANCELLED`）、
  失败任务与原因，以及该轮**每个任务**的尝试次数、起止、耗时与结果。
- **逐轮归属**：每个任务的 `attempts` 与起止时间**只反映这一轮真实发生的执行**。
  本轮没有重新跑过的任务——包括恢复轮中跨轮保留下来的 `SUCCEEDED` 任务、
  以及本轮被 `SKIPPED`/`CANCELLED` 而压根没执行的任务——在本轮记录中
  `attempts=0`、`startedAtEpochMillis=0`、`finishedAtEpochMillis=0`、
  `durationMillis=0`，不沿用上一轮的旧起止时间，也不把旧时间算成本轮耗时；
  其 `finalState` 仍如实呈现该任务在本轮结束时的状态（如上面示例中的任务 A：
  第 2 轮它并未重跑，记 0 次尝试，但状态仍是第 1 轮跑出来的 `SUCCEEDED`）。
  本轮真实执行过的任务，其起止时间必然落在本轮的
  `[startedAtEpochMillis, finishedAtEpochMillis]` 窗口内。
- **累加一致**：把同一任务在所有轮里的 `attempts` 相加，恰好等于它真实执行过的
  总次数——不会多算（保留任务不重复计数），也不会少算（每轮增量如实记录）。
  三轮及以上连续恢复时该等式同样成立。
- **只增不改**：恢复再跑一轮只是追加一条新记录，前轮内容不被覆盖、不被改写；
  已处于 `SUCCEEDED`/`CANCELLED` 终态的工作流不会再追加新轮次，
  恢复请求被幂等忽略。
- **查询稳定**：记录一旦追加即不可变，任务明细顺序与声明顺序一致；
  反复查询同一份历史，结论一致、顺序稳定。
- 被取消的执行轮同样留痕（`outcome=CANCELLED`）。

### 本地验证

```bash
# 运行历史留痕相关单测，日志会打印每轮每任务的状态流转与尝试次数依据
mvn test -Dtest=WorkflowHistoryTest
```

关键用例：

- `resumeAppendsRunRecordWithoutOverwriting`：首轮失败 → 恢复成功，
  断言第 2 轮中未重跑的任务 `attempts=0`、起止与耗时均为 0，
  且跨轮尝试累加等于真实总次数；
- `multiRoundRecoveryAccumulatesConsistently`：三轮连续恢复（FAILED→FAILED→
  SUCCEEDED），断言每轮结论/起止限定在各自轮次窗口内、前轮记录不被改写、
  逐轮尝试次数累加与真实总次数完全相等、被跳过任务不算执行过。

引擎在每轮收尾时也会打印逐任务留痕日志，可直接观察归属判定，例如：

```
[wf-1] 第2轮留痕：任务 A 本轮未重新执行（状态=SUCCEEDED），本轮尝试记 0 次、起止时间与耗时记 0，不沿用上一轮旧值
[wf-1] 第2轮留痕：任务 B 本轮真实执行，尝试=1次（累计3次），状态=SUCCEEDED，本轮耗时=2ms
```

---

## 5. 提交语义：幂等、冲突与前置校验

提交（`PUT /api/workflows`）按以下顺序判定，结论确定：

1. **前置校验**：声明非法（结构问题 → `422`；`maxConcurrency`/`maxAttempts`/
   `taskTimeoutMillis` 取值 < 1 → `400`）时在**登记之前**拒绝，
   不留下任何记录——查询返回 `404`，修正取值后同 id 可正常重新提交，
   不存在“提交失败但记录已存在”的半成品状态。
2. **幂等重复**：同 id 且声明指纹一致（任务按名称规范化排序、依赖集合排序，
   与书写顺序无关）→ 返回既有结论，`duplicateSubmission=true`，只执行一次。
3. **显式冲突**：同 id 但声明指纹不一致（任务、行为、参数或执行参数任一不同）→
   `409 DEFINITION_CONFLICT`，**不会**把对不上的旧结论当成新提交的结果返回，
   既有记录不受任何影响。

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
| `PUT /api/workflows` | 提交并同步执行；同 id 同声明幂等返回同一份记录（`duplicateSubmission=true`）；同 id 不同声明返回 `409` |
| `GET /api/workflows/{id}` | 查询不可变状态快照（含 `history` 逐轮留痕）；不存在返回 `404` |
| `POST /api/workflows/{id}/resume` | 失败恢复；非 `FAILED` 状态（含 `CANCELLED`）幂等返回当前快照 |
| `POST /api/workflows/{id}/cancel` | 取消：执行中安全收敛、`FAILED` 直接转终态；幂等；不存在返回 `404` |

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
{"error": "DEFINITION_CONFLICT",
 "message": "workflow 'demo-1' already exists with a different definition; ..."}
```

### 兼容范围

- 快照 JSON 新增 `history` 字段（纯新增，旧字段不变）；`WorkflowState` 新增
  `CANCELLED`、`TaskState` 新增 `CANCELLED` 枚举值——按状态做分支的调用方
  应容忍未知枚举值。
- 既有行为不变：提交/查询/恢复接口路径与语义、校验拒绝原因码、重试/超时/
  并发约束均保持原样；唯一的语义收紧是**同 id 不同声明**从“按重复提交处理”
  改为显式 `409`（此前会错误返回对不上的旧结论）。

---

## 9. 本地验证方法

环境：JDK 21+、Maven 3.9+。

### 9.1 运行全部测试（推荐，无需外部依赖）

```bash
mvn test
```

共 33 个用例，覆盖：

| 测试类 | 覆盖场景 |
|---|---|
| `WorkflowValidatorTest` | 同名任务、未知依赖、循环依赖、自依赖被拒（原因可区分）；合法 DAG 通过 |
| `WorkflowRetryTest` | 重试耗尽（尝试次数恰为上限、下游 SKIPPED）；临时错误重试成功；超时硬上限 |
| `WorkflowConcurrencyTest` | 并发上限=2（峰值恰好 2、时间窗重叠）；上限=1 严格串行 |
| `WorkflowDependencyOrderTest` | 菱形依赖执行顺序满足依赖，互不依赖任务并发 |
| `WorkflowRecoveryTest` | 失败后恢复成功（A 不重跑/不重复计数）；恢复仍失败时结论稳定 |
| `WorkflowIdempotencyTest` | 顺序重复提交、8 线程并发提交只产生一次执行；非法提交可反复拒绝且不留记录 |
| `WorkflowCancelTest` | 执行中取消（在途收敛、未启动 CANCELLED、结果保留）；取消幂等且终态不可恢复；FAILED/SUCCEEDED 的取消语义；未登记 404 |
| `WorkflowHistoryTest` | 失败+恢复追加两轮留痕且前轮不被覆盖、未重跑任务记 0 次尝试；三轮连续恢复的逐轮窗口与跨轮累加一致性；反复查询历史一致、顺序稳定；取消轮同样留痕 |
| `WorkflowSubmissionSemanticsTest` | 同 id 同声明幂等；同 id 不同声明显式冲突且原记录不污染；非法取值登记前拦截、修正后可重新提交 |
| `WorkflowApiSmokeTest` | 真实 HTTP：422/400/409/404、成功、查询、幂等、失败→恢复→成功、取消全链路、历史留痕 |

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

# 5) 历史留痕：demo-2 的 history 有两轮记录（FAILED 后追加 SUCCEEDED）
curl -s localhost:8080/api/workflows/demo-2 | jq '.history[] | {runNumber, outcome}'

# 6) 提交冲突：同 id 不同声明 -> 409 DEFINITION_CONFLICT
curl -s -X PUT localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"demo-1","tasks":[
    {"name":"A","dependsOn":[],"behavior":"ALWAYS_FAIL","params":[]}]}'

# 7) 非法取值 -> 400 且不留记录（查询 404，修正后可重新提交）
curl -s -X PUT localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"demo-3","maxConcurrency":0,"tasks":[
    {"name":"A","dependsOn":[],"behavior":"ALWAYS_SUCCEED","params":[]}]}'
curl -s localhost:8080/api/workflows/demo-3   # 404

# 8) 取消：慢任务执行中发起取消 -> CANCELLED 终态；resume 不可拉起
curl -s -X PUT localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"demo-4","maxConcurrency":2,"maxAttempts":3,
  "tasks":[
    {"name":"A","dependsOn":[],"behavior":"ALWAYS_SUCCEED","params":["3000"]},
    {"name":"B","dependsOn":["A"],"behavior":"ALWAYS_SUCCEED","params":[]}]}' &
sleep 1
curl -s -X POST localhost:8080/api/workflows/demo-4/cancel
curl -s -X POST localhost:8080/api/workflows/demo-4/resume   # 仍 CANCELLED，终态稳定
```

### 9.3 复现性说明

所有任务行为均为确定性逻辑（无随机数、无真实 IO），时间相关用例只断言
有界区间与状态/计数，不对墙钟时间做精确断言，因此多次运行结论一致。
