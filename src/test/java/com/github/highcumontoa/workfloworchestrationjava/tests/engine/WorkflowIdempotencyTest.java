package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.config.WorkflowProperties;
import com.github.highcumontoa.workfloworchestrationjava.engine.DeterministicTaskHandler;
import com.github.highcumontoa.workfloworchestrationjava.engine.TaskExecutor;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.model.RejectReason;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService;
import com.github.highcumontoa.workfloworchestrationjava.validation.ValidationException;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowIdempotencyTest.class);

    private WorkflowService newService() {
        WorkflowProperties props = new WorkflowProperties();
        props.setRetryBackoffMillis(0);
        props.setTaskTimeoutMillis(5000);
        props.setMaxAttempts(1);
        props.setMaxConcurrency(2);
        TaskExecutor executor = new TaskExecutor(new DeterministicTaskHandler(), props);
        WorkflowEngine engine = new WorkflowEngine(new WorkflowValidator(), executor, props);
        return new WorkflowService(engine);
    }

    private WorkflowDefinition slowWorkflow(String id) {
        return WorkflowFixtures.workflow(id,
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of("200"))),
                1, 1, 5000L);
    }

    @Test
    @DisplayName("顺序重复提交：第二次返回同一份记录并标记 duplicateSubmission，不产生第二次执行")
    void sequentialDuplicateSubmitIsIdempotent() {
        WorkflowService service = newService();
        WorkflowSnapshot first = service.submit(slowWorkflow("idem-1"));
        WorkflowSnapshot second = service.submit(slowWorkflow("idem-1"));
        log.info("[测试断言] 首次 runCount={},duplicate={}；重复提交 runCount={},duplicate={}",
                first.runCount(), first.duplicateSubmission(),
                second.runCount(), second.duplicateSubmission());
        assertFalse(first.duplicateSubmission());
        assertTrue(second.duplicateSubmission());
        assertEquals(1, second.runCount(), "重复提交不得产生第二份/第二次执行");
        assertEquals(WorkflowState.SUCCEEDED, second.state());
    }

    @Test
    @DisplayName("并发重复提交：多个线程同时提交同一 id，仅产生一次执行")
    void concurrentDuplicateSubmitRunsOnce() throws Exception {
        WorkflowService service = newService();
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<WorkflowSnapshot> results = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Exception> errors = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    results.add(service.submit(slowWorkflow("idem-2")));
                } catch (Exception e) {
                    errors.add(e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertTrue(errors.isEmpty(), "并发提交不应抛异常: " + errors);

        long originals = results.stream().filter(s -> !s.duplicateSubmission()).count();
        long duplicates = results.stream().filter(WorkflowSnapshot::duplicateSubmission).count();
        log.info("[测试断言] {} 个并发提交：原始执行={}（必须恰好1），幂等返回={}",
                threads, originals, duplicates);
        assertEquals(1, originals, "并发提交只能产生一份执行记录");
        assertEquals(threads - 1, duplicates);
        assertTrue(results.stream().allMatch(s -> s.runCount() == 1));
        assertTrue(results.stream().allMatch(s -> s.state() == WorkflowState.SUCCEEDED));
    }

    @Test
    @DisplayName("非法定义（循环依赖）无论提交多少次都被拒绝，且不会登记任何记录")
    void invalidSubmissionIsAlwaysRejected() {
        WorkflowService service = newService();
        com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition a =
                new com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition(
                        "A", Set.of("B"), "ALWAYS_SUCCEED", List.of());
        com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition b =
                new com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition(
                        "B", Set.of("A"), "ALWAYS_SUCCEED", List.of());
        WorkflowDefinition cyclic = WorkflowFixtures.workflow("bad", List.of(a, b),
                null, null, null);

        ValidationException ex1 = org.junit.jupiter.api.Assertions.assertThrows(
                ValidationException.class, () -> service.submit(cyclic));
        ValidationException ex2 = org.junit.jupiter.api.Assertions.assertThrows(
                ValidationException.class, () -> service.submit(cyclic));
        log.info("[测试断言] 两次非法提交均被拒绝：reason={}/{}，结论稳定可复现",
                ex1.getReason(), ex2.getReason());
        assertEquals(RejectReason.CYCLIC_DEPENDENCY, ex1.getReason());
        assertEquals(RejectReason.CYCLIC_DEPENDENCY, ex2.getReason());

        org.junit.jupiter.api.Assertions.assertThrows(
                com.github.highcumontoa.workfloworchestrationjava.service.WorkflowNotFoundException.class,
                () -> service.query("bad"));
    }
}
