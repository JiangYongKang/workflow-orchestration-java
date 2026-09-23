package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 工作流执行引擎。
 *
 * <p>语义：
 * <ul>
 *   <li>按拓扑顺序调度：只有全部依赖 SUCCEEDED 的任务才有资格执行；</li>
 *   <li>受限并发：通过信号量保证同时在执行的任务尝试数不超过 maxConcurrency；</li>
 *   <li>有限重试：单次临时错误/超时可重试，总尝试次数到 maxAttempts 即稳定失败；
 *       永久错误立即失败，不消耗重试预算；</li>
 *   <li>稳定失败：任一任务失败，工作流进入 FAILED，未开始的依赖保持 PENDING，
 *       绝不报告部分成功；</li>
 *   <li>断点恢复：再次 run 时已 SUCCEEDED 的任务直接跳过，不重复执行、不重复计数。</li>
 * </ul>
 */
@Component
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private final ExecutionConfig config;
    private final TaskExecutor taskExecutor;

    private final AtomicInteger currentConcurrency = new AtomicInteger();
    private final AtomicInteger peakConcurrency = new AtomicInteger();

    public WorkflowEngine(ExecutionConfig config, TaskExecutor taskExecutor) {
        this.config = config;
        this.taskExecutor = taskExecutor;
    }

    /** 本次运行观察到的同时执行任务峰值（确定性验证并发上限用）。 */
    public int getPeakConcurrency() {
        return peakConcurrency.get();
    }

    /**
     * 驱动一个已登记的工作流实例跑到终态（成功或稳定失败）。
     * 已 SUCCEEDED 的任务不会被重复执行。
     */
    public void run(WorkflowInstance instance, List<String> topoOrder) {
        synchronized (instance) {
            if (instance.getState() == WorkflowState.RUNNING) {
                log.warn("workflow '{}' is already RUNNING, skip duplicate drive",
                        instance.getSpec().id());
                return;
            }
            if (instance.getState() == WorkflowState.SUCCEEDED) {
                log.info("workflow '{}' already SUCCEEDED, idempotent no-op",
                        instance.getSpec().id());
                return;
            }
            log.info("[state-basis] workflow '{}' -> RUNNING (completed tasks will not re-run)",
                    instance.getSpec().id());
            instance.setState(WorkflowState.RUNNING);
        }

        boolean ok = drive(instance, topoOrder);

        synchronized (instance) {
            if (ok) {
                instance.setState(WorkflowState.SUCCEEDED);
                instance.setFailureReason(null);
                log.info("[state-basis] workflow '{}' -> SUCCEEDED: every task reached SUCCEEDED",
                        instance.getSpec().id());
            } else {
                instance.setState(WorkflowState.FAILED);
                log.warn("[state-basis] workflow '{}' -> FAILED: stable failure; pending dependents "
                        + "stay PENDING and are never reported as success", instance.getSpec().id());
            }
            instance.notifyAll();
        }
    }

    /**
     * 调度主循环。所有共享进度（在飞数、未满足依赖数、失败标记）都在 instance 锁内变更；
     * worker 结束时在锁内完成结算并直接拉起因此就绪的下游，形成并发接力。
     */
    private boolean drive(WorkflowInstance instance, List<String> topoOrder) {
        Map<String, TaskInstance> tasks = instance.getTasks();
        Semaphore permits = new Semaphore(config.maxConcurrency(), true);
        Deque<String> ready = new ArrayDeque<>();
        Map<String, Integer> unmet = new HashMap<>();
        Set<String> failed = new HashSet<>();

        synchronized (instance) {
            for (String id : topoOrder) {
                TaskInstance t = tasks.get(id);
                if (t.getState() == TaskState.SUCCEEDED) {
                    // 断点恢复：已完成任务不重复执行、不重复计数
                    unmet.put(id, 0);
                    continue;
                }
                if (t.getState() == TaskState.FAILED) {
                    t.setRunEpoch(t.getRunEpoch() + 1);
                    log.info("[state-basis] resume: task '{}' FAILED -> PENDING (epoch={}, attempts so far={}, kept for cap accounting)",
                            id, t.getRunEpoch(), t.getTotalAttempts());
                    t.setState(TaskState.PENDING);
                }
                long count = t.getSpec().dependencies() == null ? 0
                        : t.getSpec().dependencies().stream()
                        .filter(dep -> tasks.get(dep).getState() != TaskState.SUCCEEDED)
                        .count();
                unmet.put(id, (int) count);
                if (count == 0 && t.getState() == TaskState.PENDING) {
                    ready.add(id);
                }
            }
            instance.setFailureReason(null);
        }

        while (true) {
            while (true) {
                String id;
                synchronized (instance) {
                    if (failed.isEmpty() && !ready.isEmpty()) {
                        id = ready.poll();
                    } else {
                        break;
                    }
                }
                permits.acquireUninterruptibly();
                int active = currentConcurrency.incrementAndGet();
                peakConcurrency.accumulateAndGet(active, Math::max);
                synchronized (instance) {
                    tasks.get(id).setState(TaskState.RUNNING);
                }
                log.info("[state-basis] task '{}' dispatched (active={}, cap={})",
                        id, active, config.maxConcurrency());
                Thread worker = new Thread(
                        () -> runTask(instance, id, permits, ready, unmet, failed),
                        "workflow-" + instance.getSpec().id() + "-" + id);
                worker.setDaemon(true);
                worker.start();
            }

            synchronized (instance) {
                // 无论是否已出现失败，都先排空在飞任务：失败后不再放行新任务，
                // 已在飞的任务跑完并结算，结论才稳定可复现
                while (countInFlight(tasks) > 0) {
                    try {
                        instance.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "engine interrupted for workflow " + instance.getSpec().id(), e);
                    }
                }
                if (!failed.isEmpty()) {
                    return false;
                }
                if (ready.isEmpty()) {
                    boolean allSucceeded = topoOrder.stream()
                            .allMatch(id2 -> tasks.get(id2).getState() == TaskState.SUCCEEDED);
                    if (allSucceeded) {
                        return true;
                    }
                    log.error("[state-basis] workflow '{}' has no runnable task but unfinished work; "
                            + "treated as stable failure", instance.getSpec().id());
                    return false;
                }
            }
        }
    }

    private int countInFlight(Map<String, TaskInstance> tasks) {
        return (int) tasks.values().stream().filter(t -> t.getState() == TaskState.RUNNING).count();
    }

    private void runTask(WorkflowInstance instance, String id, Semaphore permits,
                         Deque<String> ready, Map<String, Integer> unmet, Set<String> failed) {
        TaskInstance task = instance.getTasks().get(id);
        boolean terminalSuccess = false;
        try {
            while (true) {
                int attemptNo;
                synchronized (task) {
                    if (task.getTotalAttempts() >= config.maxAttempts()) {
                        task.setLastError("retry budget exhausted: " + config.maxAttempts()
                                + " attempts reached");
                        break;
                    }
                    attemptNo = task.getTotalAttempts() + 1;
                    task.setTotalAttempts(attemptNo);
                }

                AttemptResult result;
                try {
                    result = taskExecutor.execute(task, attemptNo, config.taskTimeoutMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    synchronized (task) {
                        task.setLastError("interrupted while executing attempt " + attemptNo);
                    }
                    break;
                }

                if (result.success()) {
                    synchronized (task) {
                        task.setState(TaskState.SUCCEEDED);
                        task.setLastError(null);
                    }
                    log.info("[state-basis] task '{}' -> SUCCEEDED after {} attempt(s)", id, attemptNo);
                    terminalSuccess = true;
                    break;
                }

                synchronized (task) {
                    task.setLastError(result.errorMessage());
                }
                if (!result.retryable()) {
                    log.warn("[state-basis] task '{}' permanent error on attempt {}; no retry",
                            id, attemptNo);
                    break;
                }
                if (attemptNo >= config.maxAttempts()) {
                    log.warn("[state-basis] task '{}' retry exhausted at {} attempts (cap={}); stable failure",
                            id, attemptNo, config.maxAttempts());
                    break;
                }
                log.info("[state-basis] task '{}' transient failure attempt {}/{}, backoff {}ms",
                        id, attemptNo, config.maxAttempts(), config.retryBackoffMillis());
                if (config.retryBackoffMillis() > 0) {
                    try {
                        Thread.sleep(config.retryBackoffMillis());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        synchronized (task) {
                            task.setLastError("interrupted during retry backoff");
                        }
                        break;
                    }
                }
            }
        } finally {
            currentConcurrency.decrementAndGet();
            permits.release();
            settle(instance, id, terminalSuccess, ready, unmet, failed);
        }
    }

    /** worker 结束时的唯一结算点：状态落定、下游放行、失败熔断、唤醒主循环。 */
    private void settle(WorkflowInstance instance, String id, boolean success,
                        Deque<String> ready, Map<String, Integer> unmet, Set<String> failed) {
        synchronized (instance) {
            TaskInstance task = instance.getTasks().get(id);
            if (success) {
                task.setState(TaskState.SUCCEEDED);
                for (Map.Entry<String, Integer> e : unmet.entrySet()) {
                    String downstream = e.getKey();
                    TaskInstance dt = instance.getTasks().get(downstream);
                    boolean dependsOnMe = dt.getSpec().dependencies() != null
                            && dt.getSpec().dependencies().contains(id);
                    if (dependsOnMe) {
                        int left = e.getValue() - 1;
                        unmet.put(downstream, left);
                        if (left == 0 && dt.getState() == TaskState.PENDING && failed.isEmpty()) {
                            ready.add(downstream);
                            log.info("[state-basis] task '{}' became ready after dependency '{}' succeeded",
                                    downstream, id);
                        }
                    }
                }
            } else {
                task.setState(TaskState.FAILED);
                failed.add(id);
                String reason = task.getLastError() == null ? "task failed" : task.getLastError();
                if (instance.getFailureReason() == null) {
                    instance.setFailureReason("task '" + id + "' failed: " + reason);
                }
                log.warn("[state-basis] task '{}' -> FAILED ({}); workflow will settle as FAILED", id, reason);
            }
            log.info("[state-basis] task '{}' settled, active permits released", id);
            instance.notifyAll();
        }
    }
}
