package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.config.WorkflowProperties;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskContext;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskOutcome;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 单任务执行器。
 *
 * <p>语义（结论确定、可复现）：
 * <ul>
 *   <li>每次失败（含超时）后，只要尝试次数未达上限就退避后重试，尝试次数从 1 开始计数；</li>
 *   <li>单次尝试由看门狗施加 {@code timeoutMillis} 的硬上限，超时即中断任务并记为一次失败，
 *       不得无限阻塞；</li>
 *   <li>达到 {@code maxAttempts} 仍失败：任务进入 {@link TaskState#FAILED}，返回 false；
 *       任一任务成功：进入 {@link TaskState#SUCCEEDED}，返回 true。</li>
 * </ul>
 */
@Component("workflowTaskExecutor")
public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private final TaskHandler taskHandler;
    private final WorkflowProperties properties;
    /** 看门狗工作线程：承载可能被超时中断的任务尝试。 */
    private final ExecutorService watchdogPool =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "task-attempt-watchdog");
                t.setDaemon(true);
                return t;
            });

    public TaskExecutor(TaskHandler taskHandler, WorkflowProperties properties) {
        this.taskHandler = taskHandler;
        this.properties = properties;
    }

    /**
     * 在已持有工作流并发许可的前提下执行一个任务（含全部重试）。
     *
     * @param cancelRequested 工作流取消标志；两次尝试之间观测到即停止重试、安全收敛
     * @return 任务最终是否成功
     */
    boolean runTask(TaskRuntime task, String workflowId, int maxAttempts, long timeoutMillis,
                    java.util.function.BooleanSupplier cancelRequested)
            throws InterruptedException {
        String name = task.getDefinition().getName();
        task.setStartMillis(System.currentTimeMillis());
        // RUNNING 状态由调度器在持锁、拿到并发许可时设置，这里不重复流转
        log.info("[{}] 任务 {} 开始执行（状态=RUNNING）", workflowId, name);

        try {
            while (true) {
                int attempt = task.incrementAttempts();
                TaskContext ctx = new TaskContext(workflowId, name, attempt,
                        task.getDefinition().getBehavior(),
                        task.getDefinition().getParams());
                log.info("[{}] 任务 {} 开始第 {}/{} 次尝试，单尝试超时上限={}ms",
                        workflowId, name, attempt, maxAttempts, timeoutMillis);

                long attemptStart = System.currentTimeMillis();
                String attemptMessage = invokeWithTimeout(ctx, timeoutMillis, attemptStart);
                if (attemptMessage == null) {
                    long duration = System.currentTimeMillis() - task.getStartMillis();
                    task.setFinishMillis(System.currentTimeMillis());
                    task.setState(TaskState.SUCCEEDED);
                    task.setLastMessage("succeeded on attempt " + attempt);
                    log.info("[{}] 任务 {} 状态流转 RUNNING -> SUCCEEDED（第{}次尝试成功，累计耗时{}ms）",
                            workflowId, name, attempt, duration);
                    return true;
                }

                task.setLastMessage(attemptMessage);
                if (attempt >= maxAttempts) {
                    task.setFinishMillis(System.currentTimeMillis());
                    task.setState(TaskState.FAILED);
                    log.warn("[{}] 任务 {} 状态流转 RUNNING -> FAILED，依据=重试次数已达上限{}，最后原因={}",
                            workflowId, name, maxAttempts, attemptMessage);
                    return false;
                }
                if (cancelRequested.getAsBoolean()) {
                    // 取消期间安全收敛：当前尝试已结束，不再发起新的尝试
                    task.setFinishMillis(System.currentTimeMillis());
                    task.setState(TaskState.CANCELLED);
                    task.setLastMessage("cancelled after attempt " + attempt
                            + ": " + attemptMessage);
                    log.warn("[{}] 任务 {} 状态流转 RUNNING -> CANCELLED，依据=收到取消请求，"
                                    + "第{}次尝试已收敛，不再重试（已尝试{}次的结果保留）",
                            workflowId, name, attempt, attempt);
                    return false;
                }
                log.info("[{}] 任务 {} 第{}次尝试失败（{}），未达上限{}，退避{}ms 后重试",
                        workflowId, name, attempt, attemptMessage, maxAttempts,
                        properties.getRetryBackoffMillis());
                sleepCancellable(properties.getRetryBackoffMillis(), cancelRequested);
            }
        } finally {
            if (task.getState() == TaskState.RUNNING) {
                // 被外部中断（如关闭）：稳定落到 FAILED，绝不悬空
                task.setFinishMillis(System.currentTimeMillis());
                task.setState(TaskState.FAILED);
                task.setLastMessage("interrupted before reaching a terminal state");
                log.warn("[{}] 任务 {} 状态流转 RUNNING -> FAILED，依据=执行被中断",
                        workflowId, name);
            }
        }
    }

    /** 可被取消提前打断的退避：分片睡眠并观测取消标志，保证取消后及时收敛。 */
    private void sleepCancellable(long backoffMillis,
                                  java.util.function.BooleanSupplier cancelRequested)
            throws InterruptedException {
        long remaining = backoffMillis;
        while (remaining > 0 && !cancelRequested.getAsBoolean()) {
            long slice = Math.min(remaining, 50);
            Thread.sleep(slice);
            remaining -= slice;
        }
    }

    /**
     * 执行单次尝试并施加超时硬上限。
     *
     * @return null 表示成功；非空字符串表示本次尝试失败的原因（含超时）
     */
    private String invokeWithTimeout(TaskContext ctx, long timeoutMillis, long attemptStart)
            throws InterruptedException {
        Future<TaskOutcome> future = watchdogPool.submit(() -> taskHandler.execute(ctx));
        try {
            TaskOutcome outcome = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            return outcome.isSuccess() ? null : outcome.getMessage();
        } catch (TimeoutException e) {
            future.cancel(true);
            long elapsed = System.currentTimeMillis() - attemptStart;
            log.warn("[{}] 任务 {} 第{}次尝试超时：实际等待已超过单任务上限{}ms，中断该尝试",
                    ctx.getWorkflowId(), ctx.getTaskName(), ctx.getAttempt(),
                    timeoutMillis);
            return "attempt timed out after " + timeoutMillis
                    + "ms (elapsed~" + elapsed + "ms)";
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.warn("[{}] 任务 {} 第{}次尝试抛出未受控异常: {}",
                    ctx.getWorkflowId(), ctx.getTaskName(), ctx.getAttempt(),
                    cause.toString());
            return "handler threw: " + cause;
        }
    }

}
