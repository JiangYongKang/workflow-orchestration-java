package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowExecution;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowRetryTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRetryTest.class);

    @Test
    @DisplayName("重试耗尽：ALWAYS_FAIL 任务尝试次数恰好为上限，工作流稳定失败")
    void retriesAreBoundedAndWorkflowFails() {
        WorkflowEngine engine = EngineTestSupport.newEngine();
        WorkflowDefinition wf = WorkflowFixtures.workflow("retry-exhaust",
                List.of(
                        WorkflowFixtures.task("flaky", "ALWAYS_FAIL", List.of()),
                        WorkflowFixtures.task("down", "ALWAYS_SUCCEED", List.of(), "flaky")
                ), 2, 3, 500L);
        WorkflowExecution execution = engine.register(wf);
        engine.execute(execution);

        WorkflowSnapshot s = engine.snapshot("retry-exhaust").orElseThrow();
        log.info("[测试断言] 触发原因=flaky 行为 ALWAYS_FAIL；结论 workflowState={}，失败任务={}，原因={}",
                s.state(), s.failureTask(), s.failureReason());
        assertEquals(WorkflowState.FAILED, s.state());
        assertEquals("flaky", s.failureTask());

        var flaky = s.tasks().stream().filter(t -> t.name().equals("flaky")).findFirst().orElseThrow();
        var down = s.tasks().stream().filter(t -> t.name().equals("down")).findFirst().orElseThrow();
        log.info("[测试断言] flaky attempts={}（必须恰好等于上限3，不得无限执行），state={}；下游 down state={}",
                flaky.attempts(), flaky.state(), down.state());
        assertEquals(3, flaky.attempts());
        assertEquals(TaskState.FAILED, flaky.state());
        assertEquals(TaskState.SKIPPED, down.state());
        assertEquals(0, down.attempts(), "下游任务未被派发，尝试次数必须为 0");
        assertTrue(s.failureReason().contains("ALWAYS_FAIL"),
                "失败原因必须可解释: " + s.failureReason());
        assertFalse(s.duplicateSubmission());
    }

    @Test
    @DisplayName("临时错误在前 N 次后恢复：FAIL_FIRST_N 在第 N+1 次尝试成功，工作流成功")
    void transientFailureRecoversWithinAttempts() {
        WorkflowEngine engine = EngineTestSupport.newEngine();
        WorkflowDefinition wf = WorkflowFixtures.workflow("retry-ok",
                List.of(WorkflowFixtures.task("t", "FAIL_FIRST_N", List.of("2", "0"))),
                1, 3, 500L);
        WorkflowExecution execution = engine.register(wf);
        engine.execute(execution);

        WorkflowSnapshot s = engine.snapshot("retry-ok").orElseThrow();
        var t = s.tasks().get(0);
        log.info("[测试断言] 触发原因=FAIL_FIRST_N(2)；t attempts={}，state={}，workflowState={}",
                t.attempts(), t.state(), s.state());
        assertEquals(3, t.attempts());
        assertEquals(TaskState.SUCCEEDED, t.state());
        assertEquals(WorkflowState.SUCCEEDED, s.state());
    }

    @Test
    @DisplayName("单任务耗时上限：TIMEOUT 行为每次尝试都被看门狗中断，重试耗尽后失败")
    void timeoutIsBoundedAndCountsAsAttempt() {
        WorkflowEngine engine = EngineTestSupport.newEngine();
        WorkflowDefinition wf = WorkflowFixtures.workflow("timeout",
                List.of(WorkflowFixtures.task("slow", "TIMEOUT", List.of())),
                1, 2, 150L);
        long begin = System.currentTimeMillis();
        WorkflowExecution execution = engine.register(wf);
        engine.execute(execution);
        long elapsed = System.currentTimeMillis() - begin;

        WorkflowSnapshot s = engine.snapshot("timeout").orElseThrow();
        var slow = s.tasks().get(0);
        log.info("[测试断言] 触发原因=TIMEOUT；slow attempts={}，state={}，总耗时={}ms（必须远小于无限阻塞）",
                slow.attempts(), slow.state(), elapsed);
        assertEquals(2, slow.attempts());
        assertEquals(TaskState.FAILED, slow.state());
        assertEquals(WorkflowState.FAILED, s.state());
        assertTrue(slow.message().contains("timed out"),
                "失败原因必须标明超时: " + slow.message());
        // 2 次尝试 * 150ms 上限，留足调度余量但必须有硬上界
        assertTrue(elapsed < 2000, "超时必须被强制收敛，实际耗时=" + elapsed);
    }
}
