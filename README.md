# 轻量工作流编排（workflow-orchestration-java）

一套可在本地完整验证的轻量工作流编排能力：以**受控任务执行单元**确定性地模拟任务依赖、
临时/永久失败、超时与失败恢复，使提交、查询、恢复给出的结论一致、可解释、可复现。

## 1. 模型与状态流转

### 工作流状态 `WorkflowState`

```
PENDING ──提交/恢复开始──▶ RUNNING ──全部任务 SUCCEEDED──▶ SUCCEEDED（终态）
                              │
                              └──任一任务稳定失败，排空在飞任务后──▶ FAILED
                                                                     │
                                          POST /resume（新运行轮次）──┘
```

- **PENDING**：已登记，尚未驱动。
- **RUNNING**：正在调度执行；同一工作流的执行由按 id 分桶的运行锁串行化。
- **SUCCEEDED**：稳定终态。重复提交、重复恢复均为幂等空操作，结论不变。
- **FAILED**：稳定失败终态。未开始的下游任务保持 PENDING，**绝不会在部分任务失败时报告成功**；
  失败原因记录在 `failureReason`。恢复时引擎把上一轮 FAILED 的任务重置为 PENDING（运行轮次 +1），
  已 SUCCEEDED 的任务直接跳过，从断点继续。

### 任务状态 `TaskState`

```
PENDING ──获得并发许可、依赖全部满足──▶ RUNNING
   ▲                                      ├──成功──────────▶ SUCCEEDED（跨恢复保留，不重复执行）
   │恢复时 FAILED→PENDING                 ├──永久错误──────▶ FAILED
   └──────────────────────────────────────└──临时错误/超时，未达尝试上限─┐（退避后回到重试）
                                         └──临时错误/超时，已达尝试上限─▶ FAILED
```

- `totalAttempts`：累计尝试次数，**跨恢复轮次保留**（已完成任务不重复计数；恢复不会绕过尝试上限）。
- `runEpoch`：运行轮次，0 为首轮，每次恢复 +1，供受控任务区分首次运行与恢复运行。
- `lastError`：最近一次失败原因，成功后清空。

## 2. 受控任务行为（确定性、可复现）

任务通过 `action` 声明行为，不依赖真实随机故障：

| action | 行为 |
|---|---|
| `SUCCEED` | 始终成功（`sleepMillis` 可模拟耗时/超时） |
| `FAIL` | 始终**永久**失败：立即 FAILED，不消耗重试预算 |
| `TRANSIENT_FAIL` | 始终**临时**失败：可重试，达到尝试上限后稳定失败 |
| `FLAKY` | 前 `failTimes` 次尝试临时失败，之后成功 |
| `RECOVERABLE_FAIL` | 首轮（epoch 0）永久失败——重试无法救回，工作流先进入稳定 FAILED；恢复后的新轮次成功。用于确定性演示失败恢复 |

## 3. 重试与并发语义

- **依赖调度**：Kahn 拓扑排序产生执行顺序；只有全部依赖 SUCCEEDED 的任务才可执行。
- **受限并发**：互不依赖的任务可并发；公平信号量保证同时在执行的任务数**绝不超过
  `max-concurrency`**（测试通过观察实际峰值断言）。
- **有限重试**：临时错误/超时每次尝试计入预算；达到 `max-attempts`（首次执行 + 重试）即稳定失败，
  **不存在无限执行**。永久错误立即失败，不重试。
- **单任务超时**：单次尝试超过 `task-timeout-millis` 即中断并按一次**可重试**失败计数。
- **失败熔断**：出现失败后不再放行新任务，等待已在飞任务结算完毕，工作流才落定为 FAILED，
  避免结论被后续线程改写。
- **恢复**：已完成任务不重复执行、不重复计数；未完成（含失败）任务按依赖关系重新调度。
- **提交幂等**：以工作流 id 为键。相同声明重复提交（含 10 线程并发提交）返回**同一份实例**，
  只产生一份执行记录；相同 id 但声明内容不同返回 409 `DEFINITION_CONFLICT`。

## 4. 登记校验（被拒原因可区分）

提交先校验、后登记，**非法声明不会留下任何记录**。拒绝时 HTTP 400，body 中 `reason` 为：

- `INVALID_REQUEST`：空 id、空任务列表、空依赖等；
- `DUPLICATE_TASK`：同名任务重复声明；
- `UNKNOWN_DEPENDENCY`：依赖引用了不存在的任务；
- `CYCLIC_DEPENDENCY`：依赖图成环（拓扑排序无法排空）。

## 5. 可配置项（`src/main/resources/application.properties`）

| 配置键 | 默认值 | 含义 |
|---|---|---|
| `workflow.orchestration.max-concurrency` | 4 | 并发执行任务数硬上限 |
| `workflow.orchestration.max-attempts` | 3 | 单任务最大尝试次数（含首次） |
| `workflow.orchestration.retry-backoff-millis` | 50 | 临时失败后重试前固定等待毫秒 |
| `workflow.orchestration.task-timeout-millis` | 5000 | 单任务单次尝试耗时上限毫秒 |

## 6. HTTP 接口

| 方法与路径 | 说明 |
|---|---|
| `POST /api/workflows` | 提交（同步驱动到终态并返回结论）；重复提交幂等 |
| `GET  /api/workflows/{id}` | 查询当前状态；不存在返回 404 |
| `POST /api/workflows/{id}/resume` | 失败恢复，从断点继续；成功工作流上为幂等空操作 |

请求示例：

```json
{
  "id": "demo",
  "tasks": [
    {"id": "a", "dependencies": [], "action": "SUCCEED"},
    {"id": "b", "dependencies": ["a"], "action": "FLAKY", "failTimes": 2, "sleepMillis": 0}
  ]
}
```

## 7. 本地验证方法

### 7.1 运行全部测试（17 个，离线可用）

```bash
mvn test
```

覆盖场景与断言结论：

| 测试类 | 场景 |
|---|---|
| `WorkflowValidatorTests` | 循环依赖 / 未知依赖 / 同名任务被拒（原因分别可区分）、合法菱形依赖的拓扑顺序 |
| `WorkflowEngineTests` | 重试耗尽稳定失败（尝试次数精确等于上限）、FLAKY 在预算内成功、永久失败不重试、**并发峰值等于且不超过上限且耗时体现分批**、依赖顺序在并发下仍满足、超时按失败计数、失败后恢复不重放已完成任务且不重复计数、恢复不绕过尝试上限、**10 线程并发重复提交只产生一份执行记录且结论一致** |
| `WorkflowApiTests` | HTTP 全链路：成功提交/查询/重复提交、两类拒绝原因、失败→resume 成功 |

测试日志统一带两类标记，结论可解释：

- `[trigger]`：触发条件（如“始终临时失败、上限=3”“8 个独立任务、cap=2”）；
- `[state-basis]`：状态流转依据（尝试次数、并发许可获取/释放、任务与工作流状态落定及原因）。

### 7.2 启动应用手工验证

```bash
mvn spring-boot:run
# 或
mvn package -DskipTests && java -jar target/workflow-orchestration-java-1.0-SNAPSHOT.jar
```

```bash
# 合法工作流
curl -s -X POST localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "id":"demo","tasks":[
    {"id":"a","dependencies":[],"action":"SUCCEED"},
    {"id":"b","dependencies":["a"],"action":"FLAKY","failTimes":2}]}'

# 循环依赖被拒（reason=CYCLIC_DEPENDENCY）
curl -s -X POST localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "id":"c","tasks":[{"id":"a","dependencies":["b"],"action":"SUCCEED"},
                    {"id":"b","dependencies":["a"],"action":"SUCCEED"}]}'

# 首轮失败、恢复后成功
curl -s -X POST localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "id":"r","tasks":[{"id":"a","dependencies":[],"action":"RECOVERABLE_FAIL"}]}'
curl -s -X POST localhost:8080/api/workflows/r/resume
curl -s localhost:8080/api/workflows/r
```

## 8. 代码结构

```
model/       TaskAction/TaskSpec/WorkflowSpec/TaskInstance/WorkflowInstance、状态枚举
validation/  RejectReason、WorkflowValidationException、WorkflowValidator（校验+拓扑排序）
engine/      ExecutionConfig、AttemptResult、TaskExecutor（受控执行+超时）、WorkflowEngine（调度/并发/重试/恢复）
service/     WorkflowRegistry（幂等登记）、WorkflowService（提交/查询/恢复、并发串行化）
api/         WorkflowController、WorkflowViews、ApiExceptionHandler
config/      WorkflowProperties、EngineConfig（装配 ExecutionConfig）
```

> 说明：运行态保存在进程内存登记处，面向本地确定性验证；服务重启后记录不保留。
