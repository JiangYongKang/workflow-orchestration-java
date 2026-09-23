package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskAction;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 受控任务执行单元：按 {@link TaskSpec} 声明的行为确定性地产生成功/临时失败/永久失败，
 * 单次尝试受超时上限约束，超时的尝试会被中断并按一次（可重试的）失败计数。
 */
@Component("controlledTaskExecutor")
public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private final ExecutorService attemptWorkers = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger seq = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "task-attempt-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    });

    /**
     * 执行一次尝试。
     *
     * @param task          任务运行态
     * @param attemptNo     本次是第几次尝试（从 1 开始，FLAKY 行为据此判定何时转为成功）
     * @param timeoutMillis 单次尝试耗时上限
     */
    public AttemptResult execute(TaskInstance task, int attemptNo, long timeoutMillis)
            throws InterruptedException {
        TaskSpec spec = task.getSpec();
        long sleep = spec.sleepMillis() == null ? 0L : Math.max(0L, spec.sleepMillis());
        TaskAction action = spec.action() == null ? TaskAction.SUCCEED : spec.action();

        log.info("[trigger] task '{}' attempt {} start, action={}, sleep={}ms, timeout={}ms",
                spec.id(), attemptNo, action, sleep, timeoutMillis);

        Future<AttemptResult> future = attemptWorkers.submit(
                () -> runBody(spec, action, attemptNo, task.getRunEpoch()));
        try {
            AttemptResult result = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            log.info("[state-basis] task '{}' attempt {} finished -> success={}, retryable={}, error={}",
                    spec.id(), attemptNo, result.success(), result.retryable(), result.errorMessage());
            return result;
        } catch (TimeoutException e) {
            future.cancel(true);
            AttemptResult result = AttemptResult.failure(true,
                    "attempt " + attemptNo + " timed out after " + timeoutMillis + "ms");
            log.warn("[state-basis] task '{}' attempt {} exceeded {}ms cap, interrupted and counted as one failed attempt -> {}",
                    spec.id(), attemptNo, timeoutMillis, result.errorMessage());
            return result;
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            AttemptResult result = AttemptResult.failure(false,
                    "attempt " + attemptNo + " errored: " + cause);
            log.error("[state-basis] task '{}' attempt {} threw, treated as permanent failure -> {}",
                    spec.id(), attemptNo, result.errorMessage());
            return result;
        }
    }

    private AttemptResult runBody(TaskSpec spec, TaskAction action, int attemptNo, int runEpoch)
            throws InterruptedException {
        long sleep = spec.sleepMillis() == null ? 0L : Math.max(0L, spec.sleepMillis());
        if (sleep > 0) {
            Thread.sleep(sleep);
        }
        return switch (action) {
            case SUCCEED -> AttemptResult.ok();
            case FAIL -> AttemptResult.failure(false,
                    "attempt " + attemptNo + " failed with permanent error");
            case TRANSIENT_FAIL -> AttemptResult.failure(true,
                    "attempt " + attemptNo + " failed with transient error");
            case FLAKY -> {
                int failTimes = spec.failTimes() == null ? 0 : Math.max(0, spec.failTimes());
                if (attemptNo <= failTimes) {
                    yield AttemptResult.failure(true,
                            "attempt " + attemptNo + " of " + failTimes
                                    + " planned transient failures");
                }
                yield AttemptResult.ok();
            }
            case RECOVERABLE_FAIL -> {
                if (runEpoch == 0) {
                    // 首轮永久失败：重试无法救回，工作流必须先进入稳定 FAILED；恢复（epoch+1）后成功
                    yield AttemptResult.failure(false,
                            "attempt " + attemptNo + " failed with permanent error on first run "
                                    + "(recoverable after workflow resume)");
                }
                yield AttemptResult.ok();
            }
        };
    }
}
