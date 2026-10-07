package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.config.WorkflowProperties;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 工作流执行引擎。
 *
 * <p>调度语义：
 * <ul>
 *   <li>任务仅在其全部依赖均为 SUCCEEDED 时才被派发，实际执行顺序必然满足依赖关系；</li>
 *   <li>每个工作流持有一把信号量，运行中的任务数永不超过其并发上限；</li>
 *   <li>任一任务耗尽重试而 FAILED，未开始的任务一律 SKIPPED，在途任务收敛后工作流 FAILED；</li>
 *   <li>恢复执行时 SUCCEEDED 任务原样保留、不重复执行，仅重置 FAILED/SKIPPED 任务继续跑；</li>
 *   <li>所有状态流转均打印依据日志，结论确定且可复现。</li>
 * </ul>
 */
@Component
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowValidator validator;
    private final TaskExecutor taskExecutor;
    private final WorkflowProperties properties;

    private final Map<String, WorkflowExecution> executions = new ConcurrentHashMap<>();

    /** 承载工作流调度的线程池；任务尝试另有 TaskExecutor 的看门狗线程。 */
    private final ExecutorService schedulerPool;

    public WorkflowEngine(WorkflowValidator validator, TaskExecutor taskExecutor,
                          WorkflowProperties properties) {
        this.validator = validator;
        this.taskExecutor = taskExecutor;
        this.properties = properties;
        this.schedulerPool = Executors.newFixedThreadPool(
                Math.max(2, properties.getEngineThreads()),
                r -> {
                    Thread t = new Thread(r, "workflow-scheduler");
                    t.setDaemon(true);
                    return t;
                });
    }

    /** 仅校验不登记：供服务层在幂等/冲突判定前拦截非法声明，不产生任何记录。 */
    public void validate(WorkflowDefinition definition) {
        validator.validate(definition);
    }

    /** 登记新工作流：先校验，再占用 id；重复 id 抛 {@link IllegalArgumentException}。 */
    public WorkflowExecution register(WorkflowDefinition definition) {
        validator.validate(definition);
        WorkflowExecution execution = new WorkflowExecution(definition);
        if (executions.putIfAbsent(definition.getWorkflowId(), execution) != null) {
            log.warn("[{}] 登记被拒绝: 工作流 id 已存在（提交幂等应由服务层保证）",
                    definition.getWorkflowId());
            throw new IllegalArgumentException(
                    "workflow already registered: " + definition.getWorkflowId());
        }
        log.info("[{}] 工作流登记成功，状态={}，任务数={}",
                definition.getWorkflowId(), WorkflowState.REGISTERED,
                definition.getTasks().size());
        return execution;
    }

    /** 按 id 获取执行记录，不存在返回 null。 */
    public WorkflowExecution get(String workflowId) {
        return executions.get(workflowId);
    }

    /**
     * 请求取消（幂等）：
     * <ul>
     *   <li>RUNNING/REGISTERED：置取消标志，调度循环停止派发新任务、在途任务收敛后
     *       落到 CANCELLED 终态；</li>
     *   <li>FAILED：直接流转到 CANCELLED 终态，后续恢复请求不再生效；</li>
     *   <li>SUCCEEDED：已成功完成，取消不改变结论（幂等空操作）；</li>
     *   <li>CANCELLED：重复取消，幂等空操作。</li>
     * </ul>
     */
    public void requestCancel(WorkflowExecution execution) {
        String workflowId = execution.getDefinition().getWorkflowId();
        WorkflowState state = execution.getState();
        switch (state) {
            case CANCELLED -> log.info("[{}] 取消请求被幂等忽略：已处于 CANCELLED 终态", workflowId);
            case SUCCEEDED -> log.info(
                    "[{}] 取消请求被幂等忽略：工作流已 SUCCEEDED，结论不可撤销", workflowId);
            case FAILED -> {
                execution.requestCancel();
                execution.setFinishedAt(System.currentTimeMillis());
                execution.setState(WorkflowState.CANCELLED);
                execution.setFailureTask("(cancel)");
                execution.setFailureReason("cancel requested by client");
                log.warn("[{}] 工作流状态流转 FAILED -> CANCELLED，依据=收到取消请求，"
                        + "取消为稳定终态，后续恢复请求不再生效", workflowId);
            }
            default -> {
                execution.requestCancel();
                log.warn("[{}] 接受取消请求：状态={}，调度器将停止派发新任务并等待在途任务收敛",
                        workflowId, state);
            }
        }
    }

    /**
     * 取消并等待收敛：上界由既有约束推导
     * （单任务总耗时 ≤ maxAttempts × (超时 + 退避)，另加余量），不会无限等待。
     */
    public void cancelAndAwait(WorkflowExecution execution) {
        requestCancel(execution);
        WorkflowState s = execution.getState();
        if (s == WorkflowState.SUCCEEDED || s == WorkflowState.FAILED
                || s == WorkflowState.CANCELLED) {
            return;
        }
        var d = execution.getDefinition();
        long bound = (long) resolveMaxAttempts(d)
                * (resolveTimeoutMillis(d) + properties.getRetryBackoffMillis())
                + 10_000L;
        awaitTerminal(execution, bound);
    }

    /**
     * 有界等待工作流到达终态（SUCCEEDED/FAILED/CANCELLED）。
     * 由取消路径调用；上界由既有约束保证（单任务耗时与尝试次数均有界），
     * 这里的超时只是防御性兜底，返回是否已在终态。
     */
    public boolean awaitTerminal(WorkflowExecution execution, long timeoutMillis) {
        String workflowId = execution.getDefinition().getWorkflowId();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            WorkflowState s = execution.getState();
            if (s == WorkflowState.SUCCEEDED || s == WorkflowState.FAILED
                    || s == WorkflowState.CANCELLED) {
                return true;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        log.warn("[{}] 等待终态超过 {}ms（防御性兜底，正常不会到达），当前状态={}",
                workflowId, timeoutMillis, execution.getState());
        return false;
    }

    /**
     * 同步执行（首次执行或失败恢复）一个已登记工作流。
     * 调用方需保证同一执行记录不会被并发调用（服务层按 id 加锁）。
     * 已处于 CANCELLED 终态的执行记录拒绝再执行（取消不可逆）。
     */
    public void execute(WorkflowExecution execution) {
        String workflowId = execution.getDefinition().getWorkflowId();
        if (execution.getState() == WorkflowState.CANCELLED) {
            log.warn("[{}] 执行请求被拒绝：工作流已处于 CANCELLED 终态，取消不可逆", workflowId);
            return;
        }
        if (execution.getState() == WorkflowState.SUCCEEDED) {
            log.warn("[{}] 执行请求被拒绝：工作流已处于 SUCCEEDED 终态，不再追加新轮次",
                    workflowId);
            return;
        }
        int run = execution.incrementRunCount();
        long runStartedAt = System.currentTimeMillis();
        int maxConcurrency = resolveMaxConcurrency(execution.getDefinition());
        int maxAttempts = resolveMaxAttempts(execution.getDefinition());
        long timeoutMillis = resolveTimeoutMillis(execution.getDefinition());

        boolean resumed = execution.getState() == WorkflowState.FAILED;
        if (!resumed) {
            execution.setStartedAt(System.currentTimeMillis());
            log.info("[{}] 工作流开始首次执行（runCount={}）：并发上限={}，重试上限={}，单任务超时={}ms",
                    workflowId, run, maxConcurrency, maxAttempts, timeoutMillis);
        }

        Map<String, TaskRuntime> tasks = new LinkedHashMap<>();
        for (TaskDefinition td : execution.getDefinition().getTasks()) {
            tasks.put(td.getName(), new TaskRuntime(td));
        }
        // 首次执行得到全新运行态；恢复执行得到上一轮运行态（SUCCEEDED 原样保留）
        tasks = execution.attachTaskRuntimes(tasks);

        if (resumed) {
            resetForResume(execution);
            for (TaskRuntime kept : tasks.values()) {
                if (kept.getState() == TaskState.SUCCEEDED) {
                    log.info("[{}] 恢复：任务 {} 保持 SUCCEEDED（已尝试{}次），不重复执行",
                            workflowId, kept.getDefinition().getName(), kept.getAttempts());
                }
            }
            log.info("[{}] 工作流失败恢复开始（runCount={}）：已完成任务保持不变，剩余任务继续执行",
                    workflowId, run);
        }
        execution.setState(WorkflowState.RUNNING);
        execution.setFailureTask(null);
        execution.setFailureReason("");

        // 本轮基线：恢复轮中保留的 SUCCEEDED 任务带着上一轮的尝试计数与起止时间，
        // 留痕必须只记本轮增量，先把本轮起跑线记下来
        Map<String, long[]> runBaseline = new LinkedHashMap<>();
        for (TaskRuntime t : tasks.values()) {
            runBaseline.put(t.getDefinition().getName(),
                    new long[]{t.getAttempts(), t.getStartMillis(), t.getFinishMillis()});
        }

        runLoop(execution, tasks, maxConcurrency, maxAttempts, timeoutMillis, run,
                runStartedAt, runBaseline);
    }

    /**
     * 恢复时原地重置运行态：
     * FAILED 任务先回到 PENDING（计数清零）；随后按依赖闭包扩散，
     * 只要一个 SKIPPED 任务的全部依赖已处于“可重新达成”的状态
     * （SUCCEEDED 或已重置为 PENDING），它也回到 PENDING。
     * 依赖链上仍有其它 FAILED 的 SKIPPED 任务保持 SKIPPED，直到对应分支恢复。
     */
    private void resetForResume(WorkflowExecution execution) {
        String workflowId = execution.getDefinition().getWorkflowId();
        Map<String, TaskRuntime> tasks = execution.currentTaskRuntimes();

        // 1) 先重置所有 FAILED 根因任务
        for (TaskRuntime t : tasks.values()) {
            if (t.getState() == TaskState.FAILED) {
                t.setState(TaskState.PENDING);
                t.resetAttempts();
                t.setStartMillis(0);
                t.setFinishMillis(0);
                t.setLastMessage("reset for resume");
                log.info("[{}] 恢复：失败任务 {} 重置为 PENDING，尝试次数重新计数",
                        workflowId, t.getDefinition().getName());
            }
        }

        // 2) 定点扩散解锁 SKIPPED 下游
        boolean changed = true;
        while (changed) {
            changed = false;
            for (TaskRuntime t : tasks.values()) {
                if (t.getState() != TaskState.SKIPPED) {
                    continue;
                }
                boolean blocked = t.getDefinition().getDependsOn() != null
                        && t.getDefinition().getDependsOn().stream()
                        .map(d -> tasks.get(d).getState())
                        .anyMatch(st -> st == TaskState.FAILED || st == TaskState.SKIPPED);
                if (!blocked) {
                    t.setState(TaskState.PENDING);
                    t.setStartMillis(0);
                    t.setFinishMillis(0);
                    t.setLastMessage("reset for resume");
                    log.info("[{}] 恢复：被跳过任务 {} 重新置为 PENDING（依赖已可重新达成）",
                            workflowId, t.getDefinition().getName());
                    changed = true;
                }
            }
        }
    }

    private void runLoop(WorkflowExecution execution, Map<String, TaskRuntime> tasks,
                         int maxConcurrency, int maxAttempts, long timeoutMillis,
                         int run, long runStartedAt, Map<String, long[]> runBaseline) {
        String workflowId = execution.getDefinition().getWorkflowId();
        Semaphore permits = new Semaphore(maxConcurrency);
        AtomicInteger running = new AtomicInteger(0);
        AtomicInteger peak = new AtomicInteger(0);
        AtomicInteger finishSeq = new AtomicInteger(0);
        Object scheduleLock = new Object();
        // 仅在状态机层面标记失败；在途任务允许收敛
        final TaskRuntime[] firstFailure = new TaskRuntime[1];

        boolean interrupted = false;
        boolean done = false;
        while (!done) {
            synchronized (scheduleLock) {
                while (!done) {
                    boolean madeProgress = false;
                    // 1a) 取消已请求：所有尚未派发的任务直接 CANCELLED，不再启动新任务
                    if (execution.isCancelRequested()) {
                        for (TaskRuntime t : tasks.values()) {
                            if (t.getState() == TaskState.PENDING) {
                                t.setState(TaskState.CANCELLED);
                                t.setLastMessage("cancelled: workflow cancel requested");
                                log.info("[{}] 任务 {} 状态流转 PENDING -> CANCELLED，依据=收到取消请求，未启动任务不再派发",
                                        workflowId, t.getDefinition().getName());
                                madeProgress = true;
                            }
                        }
                    }
                    // 1b) 失败已注定：所有尚未派发的任务直接 SKIPPED
                    if (firstFailure[0] != null) {
                        for (TaskRuntime t : tasks.values()) {
                            if (t.getState() == TaskState.PENDING) {
                                t.setState(TaskState.SKIPPED);
                                t.setLastMessage("skipped: task '"
                                        + firstFailure[0].getDefinition().getName()
                                        + "' failed");
                                log.info("[{}] 任务 {} 状态流转 PENDING -> SKIPPED，依据=上游任务 {} 已失败",
                                        workflowId, t.getDefinition().getName(),
                                        firstFailure[0].getDefinition().getName());
                                madeProgress = true;
                            }
                        }
                    }

                    // 2) 派发所有“依赖已满足且当前有并发许可”的任务（取消/失败后不再派发）
                    for (TaskRuntime t : tasks.values()) {
                        if (t.getState() == TaskState.PENDING
                                && firstFailure[0] == null
                                && !execution.isCancelRequested()
                                && depsSatisfied(t, tasks)
                                && permits.tryAcquire()) {
                            // 许可已到手，任务才允许离开 PENDING，杜绝假死
                            dispatch(execution, t, tasks, permits, running, peak,
                                    finishSeq, scheduleLock, maxConcurrency,
                                    maxAttempts, timeoutMillis, firstFailure);
                            madeProgress = true;
                        }
                    }

                    // 3) 终止判定：不存在 PENDING，也不存在在途任务
                    if (tasks.values().stream().noneMatch(t ->
                            t.getState() == TaskState.PENDING)
                            && running.get() == 0) {
                        done = true;
                        break;
                    }

                    if (!madeProgress) {
                        // 无可派发任务：要么在等待在途任务完成，要么在等待并发许可，
                        // 二者都会在释放时 notifyAll 唤醒此处
                        try {
                            scheduleLock.wait(100);
                        } catch (InterruptedException ie) {
                            interrupted = true;
                            done = true;
                            Thread.currentThread().interrupt();
                            log.warn("[{}] 调度线程被中断，工作流按失败收敛", workflowId);
                            break;
                        }
                    }
                }
            }
        }

        // 终态判定：取消优先于失败/成功，结论确定
        execution.setPeakConcurrency(peak.get());
        execution.setFinishedAt(System.currentTimeMillis());
        if (execution.isCancelRequested()) {
            // 在途任务已收敛（running==0），防御性兜底：仍非终态的一律 CANCELLED
            for (TaskRuntime t : tasks.values()) {
                if (t.getState() == TaskState.PENDING || t.getState() == TaskState.RUNNING) {
                    t.setState(TaskState.CANCELLED);
                    t.setLastMessage("cancelled: workflow aborted");
                }
            }
            execution.setFailureTask("(cancel)");
            execution.setFailureReason("cancel requested by client");
            execution.setState(WorkflowState.CANCELLED);
            log.warn("[{}] 工作流状态流转 RUNNING -> CANCELLED，依据=收到取消请求，"
                            + "在途任务已收敛、未启动任务不再执行、已完成结果保留，峰值并发={}",
                    workflowId, peak.get());
        } else if (firstFailure[0] != null || interrupted) {
            for (TaskRuntime t : tasks.values()) {
                if (t.getState() == TaskState.PENDING || t.getState() == TaskState.RUNNING) {
                    t.setState(TaskState.SKIPPED);
                    t.setLastMessage("skipped: workflow aborted");
                }
            }
            TaskRuntime failed = firstFailure[0];
            if (failed != null) {
                execution.setFailureTask(failed.getDefinition().getName());
                execution.setFailureReason(failed.getLastMessage());
            } else {
                execution.setFailureTask("(scheduler)");
                execution.setFailureReason("scheduler interrupted");
            }
            execution.setState(WorkflowState.FAILED);
            log.warn("[{}] 工作流状态流转 RUNNING -> FAILED，依据=任务 {} 进入稳定失败（{}），峰值并发={}",
                    workflowId, execution.getFailureTask(), execution.getFailureReason(),
                    peak.get());
        } else {
            long duration = execution.getFinishedAt() - execution.getStartedAt();
            execution.setState(WorkflowState.SUCCEEDED);
            log.info("[{}] 工作流状态流转 RUNNING -> SUCCEEDED，全部任务成功，峰值并发={}，总耗时={}ms",
                    workflowId, peak.get(), duration);
        }

        appendRunRecord(execution, tasks, run, runStartedAt, runBaseline);
    }

    /**
     * 每轮收敛后追加一条不可变留痕：轮次、起止、结论与各任务该轮的
     * 尝试次数/耗时/结果。只增不改，恢复执行只是追加新一轮。
     *
     * <p>逐轮归属语义：每个任务的尝试次数与起止时间只反映<b>本轮</b>真实发生的执行——
     * 以本轮起跑线（{@code runBaseline}）为基准取增量；本轮没有重新跑过的任务
     * （如恢复轮中保留的 SUCCEEDED 任务、本轮被跳过/取消且未执行的任务），
     * 在本轮留痕中记 0 次尝试、起止时间与耗时均为 0，不沿用上一轮的旧值。
     * 因此把同一任务在所有轮里的尝试次数相加，恰好等于它真实执行过的总次数。
     */
    private void appendRunRecord(WorkflowExecution execution, Map<String, TaskRuntime> tasks,
                                 int run, long runStartedAt, Map<String, long[]> runBaseline) {
        String workflowId = execution.getDefinition().getWorkflowId();
        List<com.github.highcumontoa.workfloworchestrationjava.model.TaskRunRecord> taskRecords =
                new ArrayList<>();
        for (TaskRuntime t : tasks.values()) {
            String name = t.getDefinition().getName();
            long[] base = runBaseline.get(name);
            int attemptsThisRun = t.getAttempts() - (int) base[0];
            // 本轮是否真的跑过：尝试数有增量，或起始时间在本轮被重新写入
            boolean ranThisRun = attemptsThisRun > 0 || t.getStartMillis() != base[1];
            long start = ranThisRun ? t.getStartMillis() : 0;
            long finish = ranThisRun ? t.getFinishMillis() : 0;
            long duration = start > 0 && finish > 0 ? finish - start : 0;
            taskRecords.add(new com.github.highcumontoa.workfloworchestrationjava.model.TaskRunRecord(
                    name, t.getState(), attemptsThisRun, start, finish, duration,
                    t.getLastMessage()));
            if (ranThisRun) {
                log.info("[{}] 第{}轮留痕：任务 {} 本轮真实执行，尝试={}次（累计{}次），"
                                + "状态={}，本轮耗时={}ms",
                        workflowId, run, name, attemptsThisRun, t.getAttempts(),
                        t.getState(), duration);
            } else {
                log.info("[{}] 第{}轮留痕：任务 {} 本轮未重新执行（状态={}），"
                                + "本轮尝试记 0 次、起止时间与耗时记 0，不沿用上一轮旧值",
                        workflowId, run, name, t.getState());
            }
        }
        execution.appendHistory(new com.github.highcumontoa.workfloworchestrationjava.model.RunRecord(
                run, runStartedAt, execution.getFinishedAt(), execution.getState(),
                execution.getFailureTask(), execution.getFailureReason(),
                List.copyOf(taskRecords)));
        log.info("[{}] 第{}轮执行留痕已追加：结论={}，起={}，止={}，任务明细数={}（历史只增不改）",
                workflowId, run, execution.getState(), runStartedAt,
                execution.getFinishedAt(), taskRecords.size());
    }

    /** 许可已由调度线程持有；本方法把任务派发到工作线程，并在结束时释放许可。 */
    private void dispatch(WorkflowExecution execution, TaskRuntime task,
                          Map<String, TaskRuntime> tasks, Semaphore permits,
                          AtomicInteger running, AtomicInteger peak,
                          AtomicInteger finishSeq, Object scheduleLock,
                          int maxConcurrency, int maxAttempts,
                          long timeoutMillis, TaskRuntime[] firstFailure) {
        String workflowId = execution.getDefinition().getWorkflowId();
        task.setState(TaskState.RUNNING);
        // 在调度线程内同步预增，保证调度器立即把该任务视为“在途”，杜绝漏判提前收尾
        int now = running.incrementAndGet();
        peak.accumulateAndGet(now, Math::max);
        schedulerPool.submit(() -> {
            try {
                log.info("[{}] 任务 {} 获得并发许可（当前在途={}）",
                        workflowId, task.getDefinition().getName(),
                        permitsTrack(permits, maxConcurrency));

                boolean ok = taskExecutor.runTask(task, workflowId, maxAttempts, timeoutMillis,
                        execution::isCancelRequested);
                if (!ok) {
                    synchronized (scheduleLock) {
                        if (firstFailure[0] == null) {
                            firstFailure[0] = task;
                            log.warn("[{}] 任务 {} 已稳定失败，工作流注定 FAILED，其余未开始任务将 SKIPPED",
                                    workflowId, task.getDefinition().getName());
                        }
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                task.setState(TaskState.FAILED);
                task.setLastMessage("dispatch interrupted");
                synchronized (scheduleLock) {
                    if (firstFailure[0] == null) {
                        firstFailure[0] = task;
                    }
                }
            } catch (RuntimeException e) {
                task.setState(TaskState.FAILED);
                task.setLastMessage("dispatch error: " + e);
                synchronized (scheduleLock) {
                    if (firstFailure[0] == null) {
                        firstFailure[0] = task;
                    }
                }
            } finally {
                task.setFinishOrder(finishSeq.incrementAndGet());
                permits.release();
                running.decrementAndGet();
                synchronized (scheduleLock) {
                    scheduleLock.notifyAll();
                }
            }
        });
    }

    private String permitsTrack(Semaphore permits, int maxConcurrency) {
        return (maxConcurrency - permits.availablePermits()) + "/" + maxConcurrency;
    }

    private boolean depsSatisfied(TaskRuntime t, Map<String, TaskRuntime> tasks) {
        if (t.getDefinition().getDependsOn() == null) {
            return true;
        }
        for (String dep : t.getDefinition().getDependsOn()) {
            if (tasks.get(dep).getState() != TaskState.SUCCEEDED) {
                return false;
            }
        }
        return true;
    }

    private int resolveMaxConcurrency(WorkflowDefinition d) {
        int v = d.getMaxConcurrency() != null ? d.getMaxConcurrency() : properties.getMaxConcurrency();
        if (v < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1");
        }
        return v;
    }

    private int resolveMaxAttempts(WorkflowDefinition d) {
        int v = d.getMaxAttempts() != null ? d.getMaxAttempts() : properties.getMaxAttempts();
        if (v < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        return v;
    }

    private long resolveTimeoutMillis(WorkflowDefinition d) {
        long v = d.getTaskTimeoutMillis() != null
                ? d.getTaskTimeoutMillis() : properties.getTaskTimeoutMillis();
        if (v < 1) {
            throw new IllegalArgumentException("taskTimeoutMillis must be >= 1");
        }
        return v;
    }

    /** 查询不可变快照；不存在返回 empty。 */
    public Optional<WorkflowSnapshot> snapshot(String workflowId) {
        WorkflowExecution execution = executions.get(workflowId);
        return execution == null ? Optional.empty() : Optional.of(toSnapshot(execution, false));
    }

    public Optional<WorkflowSnapshot> snapshot(WorkflowExecution execution, boolean duplicate) {
        return Optional.of(toSnapshot(execution, duplicate));
    }

    private WorkflowSnapshot toSnapshot(WorkflowExecution execution, boolean duplicate) {
        WorkflowDefinition d = execution.getDefinition();
        Map<String, TaskRuntime> tasks = execution.currentTaskRuntimes();
        List<TaskSnapshot> list = new ArrayList<>();
        if (tasks != null) {
            for (TaskRuntime t : tasks.values()) {
                long duration = t.getFinishMillis() == 0
                        ? (t.getStartMillis() == 0 ? 0
                                : System.currentTimeMillis() - t.getStartMillis())
                        : t.getFinishMillis() - t.getStartMillis();
                list.add(new TaskSnapshot(t.getDefinition().getName(), t.getState(),
                        t.getAttempts(), t.getStartMillis(), t.getFinishMillis(),
                        duration, t.getLastMessage()));
            }
        }
        return new WorkflowSnapshot(d.getWorkflowId(), execution.getState(),
                execution.getRunCount(), resolveMaxConcurrency(d), execution.getPeakConcurrency(),
                resolveMaxAttempts(d), resolveTimeoutMillis(d),
                execution.getFailureTask(), execution.getFailureReason(),
                execution.getStartedAt(), execution.getFinishedAt(), list, duplicate,
                execution.history());
    }
}
