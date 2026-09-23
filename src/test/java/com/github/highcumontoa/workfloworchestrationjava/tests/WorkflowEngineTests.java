package com.github.highcumontoa.workfloworchestrationjava.tests;

import com.github.highcumontoa.workfloworchestrationjava.engine.AttemptResult;
import com.github.highcumontoa.workfloworchestrationjava.engine.ExecutionConfig;
import com.github.highcumontoa.workfloworchestrationjava.engine.TaskExecutor;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskAction;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskSpec;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSpec;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowRegistry;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowEngineTests {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngineTests.class);

    /** 记录每次尝试启动的任务 id，用于断言执行顺序与“不重复执行”。 */
    static class RecordingExecutor extends TaskExecutor {
        final List<String> started = Collections.synchronizedList(new ArrayList<>());

        @Override
        public AttemptResult execute(TaskInstance task, int attemptNo, long timeoutMillis)
                throws InterruptedException {
            started.add(task.getSpec().id() + "#" + attemptNo);
            return super.execute(task, attemptNo, timeoutMillis);
        }
    }

    private TaskSpec task(String id, TaskAction action, int failTimes, long sleep, String... deps) {
        return new TaskSpec(id, List.of(deps), action, failTimes, sleep);
    }

    private WorkflowInstance register(WorkflowSpec spec, WorkflowRegistry registry) {
        return registry.register(spec);
    }

    @Test
    void transientFailureRetriesUntilBudgetExhaustedThenStableFailure() {
        RecordingExecutor exec = new RecordingExecutor();
        WorkflowEngine engine = new WorkflowEngine(new ExecutionConfig(4, 3, 5, 5_000), exec);
        WorkflowSpec spec = new WorkflowSpec("wf-retry-exhaust",
                List.of(task("t", TaskAction.TRANSIENT_FAIL, 0, 0)));
        WorkflowInstance wf = register(spec, new WorkflowRegistry());

        engine.run(wf, new WorkflowValidator().validateAndOrder(spec));

        TaskInstance t = wf.getTasks().get("t");
        log.info("[trigger] task always transient-fails, cap=3 -> [state-basis] attempts={}, task={}, workflow={}, reason={}",
                t.getTotalAttempts(), t.getState(), wf.getState(), wf.getFailureReason());
        assertEquals(3, t.getTotalAttempts(), "must stop exactly at the attempt cap, never loop forever");
        assertEquals(TaskState.FAILED, t.getState());
        assertEquals(WorkflowState.FAILED, wf.getState(), "partial failure must never be reported as success");
        assertNotNull(wf.getFailureReason());
    }

    @Test
    void flakyTaskSucceedsWithinRetryBudget() {
        RecordingExecutor exec = new RecordingExecutor();
        WorkflowEngine engine = new WorkflowEngine(new ExecutionConfig(4, 3, 5, 5_000), exec);
        WorkflowSpec spec = new WorkflowSpec("wf-flaky",
                List.of(task("t", TaskAction.FLAKY, 2, 0)));
        WorkflowInstance wf = register(spec, new WorkflowRegistry());

        engine.run(wf, new WorkflowValidator().validateAndOrder(spec));

        TaskInstance t = wf.getTasks().get("t");
        log.info("[trigger] task fails first 2 attempts then succeeds -> [state-basis] attempts={}, task={}, workflow={}",
                t.getTotalAttempts(), t.getState(), wf.getState());
        assertEquals(3, t.getTotalAttempts());
        assertEquals(TaskState.SUCCEEDED, t.getState());
        assertEquals(WorkflowState.SUCCEEDED, wf.getState());
    }

    @Test
    void permanentFailureDoesNotConsumeRetryBudget() {
        RecordingExecutor exec = new RecordingExecutor();
        WorkflowEngine engine = new WorkflowEngine(new ExecutionConfig(4, 5, 5, 5_000), exec);
        WorkflowSpec spec = new WorkflowSpec("wf-permanent",
                List.of(task("t", TaskAction.FAIL, 0, 0)));
        WorkflowInstance wf = register(spec, new WorkflowRegistry());

        engine.run(wf, new WorkflowValidator().validateAndOrder(spec));

        TaskInstance t = wf.getTasks().get("t");
        log.info("[trigger] task fails permanently, cap=5 -> [state-basis] attempts={} (no retry), task={}, workflow={}",
                t.getTotalAttempts(), t.getState(), wf.getState());
        assertEquals(1, t.getTotalAttempts(), "permanent error must fail immediately without retries");
        assertEquals(TaskState.FAILED, t.getState());
        assertEquals(WorkflowState.FAILED, wf.getState());
    }

    @Test
    void concurrencyNeverExceedsConfiguredCap() {
        RecordingExecutor exec = new RecordingExecutor();
        int cap = 2;
        WorkflowEngine engine = new WorkflowEngine(new ExecutionConfig(cap, 1, 0, 5_000), exec);
        List<TaskSpec> tasks = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            tasks.add(task("t" + i, TaskAction.SUCCEED, 0, 60));
        }
        WorkflowSpec spec = new WorkflowSpec("wf-cap", tasks);
        WorkflowInstance wf = register(spec, new WorkflowRegistry());

        long start = System.nanoTime();
        engine.run(wf, new WorkflowValidator().validateAndOrder(spec));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        log.info("[trigger] 8 independent tasks sleeping 60ms with cap=2 -> [state-basis] observed peak={}, elapsed={}ms",
                engine.getPeakConcurrency(), elapsedMs);
        assertEquals(cap, engine.getPeakConcurrency(), "observed concurrency must reach but never exceed the cap");
        assertTrue(elapsedMs >= 200,
                "8 tasks / cap 2 need at least 4 waves, elapsed " + elapsedMs + "ms shows serialization");
        wf.taskList().forEach(t -> assertEquals(TaskState.SUCCEEDED, t.getState()));
        assertEquals(WorkflowState.SUCCEEDED, wf.getState());
    }

    @Test
    void dependencyOrderIsRespectedUnderConcurrency() {
        RecordingExecutor exec = new RecordingExecutor();
        WorkflowEngine engine = new WorkflowEngine(new ExecutionConfig(4, 1, 0, 5_000), exec);
        // 菱形：a 最慢必须先跑；b、c 依赖 a；d 依赖 b、c
        WorkflowSpec spec = new WorkflowSpec("wf-diamond-run", List.of(
                task("a", TaskAction.SUCCEED, 0, 120),
                task("b", TaskAction.SUCCEED, 0, 20, "a"),
                task("c", TaskAction.SUCCEED, 0, 20, "a"),
                task("d", TaskAction.SUCCEED, 0, 20, "b", "c")));
        WorkflowInstance wf = register(spec, new WorkflowRegistry());

        engine.run(wf, new WorkflowValidator().validateAndOrder(spec));

        synchronized (exec.started) {
            log.info("[trigger] diamond dependencies, a sleeps 120ms -> [state-basis] actual attempt order={}",
                    exec.started);
            assertEquals("a#1", exec.started.get(0), "dependency 'a' must execute first");
            assertEquals("d#1", exec.started.get(exec.started.size() - 1),
                    "d waits for both b and c and must execute last");
            assertTrue(exec.started.indexOf("b#1") < exec.started.indexOf("d#1"));
            assertTrue(exec.started.indexOf("c#1") < exec.started.indexOf("d#1"));
        }
        assertEquals(WorkflowState.SUCCEEDED, wf.getState());
    }

    @Test
    void attemptTimedOutCountsAsFailedAttempt() {
        RecordingExecutor exec = new RecordingExecutor();
        WorkflowEngine engine = new WorkflowEngine(new ExecutionConfig(4, 2, 5, 50), exec);
        WorkflowSpec spec = new WorkflowSpec("wf-timeout",
                List.of(task("slow", TaskAction.SUCCEED, 0, 300)));
        WorkflowInstance wf = register(spec, new WorkflowRegistry());

        engine.run(wf, new WorkflowValidator().validateAndOrder(spec));

        TaskInstance t = wf.getTasks().get("slow");
        log.info("[trigger] task body 300ms exceeds 50ms timeout cap -> [state-basis] attempts={}, lastError='{}'",
                t.getTotalAttempts(), t.getLastError());
        assertEquals(2, t.getTotalAttempts(), "each timed-out attempt is counted up to the cap");
        assertEquals(TaskState.FAILED, t.getState());
        assertTrue(t.getLastError().contains("timed out"));
        assertEquals(WorkflowState.FAILED, wf.getState());
    }

    @Test
    void resumeContinuesFromCompletedTasksWithoutReplay() {
        RecordingExecutor exec = new RecordingExecutor();
        WorkflowEngine engine = new WorkflowEngine(new ExecutionConfig(4, 3, 5, 5_000), exec);
        WorkflowSpec spec = new WorkflowSpec("wf-resume", List.of(
                task("a", TaskAction.SUCCEED, 0, 0),
                task("b", TaskAction.RECOVERABLE_FAIL, 0, 0),
                task("c", TaskAction.SUCCEED, 0, 0, "b")));
        WorkflowRegistry registry = new WorkflowRegistry();
        WorkflowInstance wf = register(spec, registry);
        List<String> order = new WorkflowValidator().validateAndOrder(spec);

        engine.run(wf, order);
        log.info("[trigger] b permanently fails on first run -> [state-basis] workflow={}, a={}, b={}, c={} (c never started)",
                wf.getState(), wf.getTasks().get("a").getState(),
                wf.getTasks().get("b").getState(), wf.getTasks().get("c").getState());
        assertEquals(WorkflowState.FAILED, wf.getState());
        assertEquals(TaskState.SUCCEEDED, wf.getTasks().get("a").getState());
        assertEquals(TaskState.FAILED, wf.getTasks().get("b").getState());
        assertEquals(TaskState.PENDING, wf.getTasks().get("c").getState(),
                "a failed dependency leaves its dependent pending, never reported as done");

        engine.run(wf, order);
        log.info("[trigger] resume after b becomes recoverable -> [state-basis] workflow={}, attempts a/b/c={}/{}/{}, started={}",
                wf.getState(),
                wf.getTasks().get("a").getTotalAttempts(),
                wf.getTasks().get("b").getTotalAttempts(),
                wf.getTasks().get("c").getTotalAttempts(),
                exec.started);
        assertEquals(WorkflowState.SUCCEEDED, wf.getState());
        assertEquals(1, wf.getTasks().get("a").getTotalAttempts(), "completed task must not be re-counted");
        assertEquals(2, wf.getTasks().get("b").getTotalAttempts(),
                "failed task gets exactly one resume attempt; the first-run attempt is kept in the count");
        assertEquals(1, wf.getTasks().get("c").getTotalAttempts());
        synchronized (exec.started) {
            assertEquals(1, Collections.frequency(exec.started, "a#1"),
                    "completed task must not be executed again on resume");
            assertTrue(exec.started.contains("c#1"));
        }
    }

    @Test
    void exhaustedBudgetIsNotInflatedByNoopResumeAttempts() {
        RecordingExecutor exec = new RecordingExecutor();
        WorkflowEngine engine = new WorkflowEngine(new ExecutionConfig(4, 2, 0, 5_000), exec);
        WorkflowSpec spec = new WorkflowSpec("wf-cap-account",
                List.of(task("t", TaskAction.TRANSIENT_FAIL, 0, 0)));
        WorkflowInstance wf = register(spec, new WorkflowRegistry());
        List<String> order = new WorkflowValidator().validateAndOrder(spec);

        engine.run(wf, order);
        engine.run(wf, order);

        TaskInstance t = wf.getTasks().get("t");
        log.info("[trigger] transient task resumed after exhaustion -> [state-basis] attempts stays={}, task={}, workflow={}",
                t.getTotalAttempts(), t.getState(), wf.getState());
        assertEquals(2, t.getTotalAttempts(), "resume must not count attempts beyond the cap");
        assertEquals(WorkflowState.FAILED, wf.getState());
    }

    @Test
    void concurrentDuplicateSubmissionsProduceSingleExecution() throws Exception {
        RecordingExecutor exec = new RecordingExecutor();
        ExecutionConfig cfg = new ExecutionConfig(2, 1, 0, 5_000);
        WorkflowEngine engine = new WorkflowEngine(cfg, exec);
        com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService service =
                new com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService(
                        new WorkflowValidator(), new WorkflowRegistry(), engine);
        WorkflowSpec spec = new WorkflowSpec("wf-idempotent", List.of(
                task("a", TaskAction.SUCCEED, 0, 40),
                task("b", TaskAction.SUCCEED, 0, 40, "a"),
                task("c", TaskAction.SUCCEED, 0, 40, "a")));

        int submitters = 10;
        ExecutorService pool = Executors.newFixedThreadPool(submitters);
        List<WorkflowState> outcomes = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < submitters; i++) {
            pool.submit(() -> {
                try {
                    outcomes.add(service.submit(spec).getState());
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        log.info("[trigger] same workflow submitted concurrently by {} threads -> [state-basis] outcomes={}, distinct attempts started={}",
                submitters, outcomes, exec.started);
        assertTrue(errors.isEmpty(), "concurrent duplicate submissions must not error: " + errors);
        assertEquals(submitters, outcomes.size());
        outcomes.forEach(s -> assertEquals(WorkflowState.SUCCEEDED, s,
                "every concurrent submitter observes the same stable conclusion"));
        assertEquals(List.of("a#1", "b#1", "c#1"), List.copyOf(exec.started),
                "duplicate submissions must not produce a second execution record");
    }
}
