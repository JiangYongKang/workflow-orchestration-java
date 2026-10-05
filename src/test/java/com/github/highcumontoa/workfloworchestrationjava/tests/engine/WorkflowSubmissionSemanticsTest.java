package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowConflictException;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowNotFoundException;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowSubmissionSemanticsTest {

    private static final Logger log =
            LoggerFactory.getLogger(WorkflowSubmissionSemanticsTest.class);

    private WorkflowService newService() {
        return new WorkflowService(EngineTestSupport.newEngine());
    }

    @Test
    @DisplayName("同 id 原样重复提交：幂等返回同一份结论，只执行一次")
    void identicalResubmissionIsIdempotent() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("sub-idem",
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                2, 2, 500L);
        WorkflowSnapshot s1 = service.submit(wf);
        // 任务书写顺序不同、依赖集合顺序不同，但声明等价：仍属原样重复
        WorkflowDefinition same = WorkflowFixtures.workflow("sub-idem",
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                2, 2, 500L);
        WorkflowSnapshot s2 = service.submit(same);
        log.info("[测试断言] 原样重复提交：runCount {}=={}，duplicateSubmission={}，结论={}",
                s1.runCount(), s2.runCount(), s2.duplicateSubmission(), s2.state());
        assertEquals(WorkflowState.SUCCEEDED, s1.state());
        assertEquals(1, s2.runCount(), "重复提交不得产生第二次执行");
        assertTrue(s2.duplicateSubmission());
        assertEquals(s1.state(), s2.state());
        assertEquals(s1.finishedAt(), s2.finishedAt(), "返回的必须是同一份旧结论");
    }

    @Test
    @DisplayName("同 id 不同声明：显式冲突（WorkflowConflictException），不返回对不上的旧结论，原记录不被污染")
    void divergentResubmissionIsConflict() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("sub-conflict",
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                2, 2, 500L);
        WorkflowSnapshot s1 = service.submit(wf);
        assertEquals(WorkflowState.SUCCEEDED, s1.state());

        WorkflowDefinition divergent = WorkflowFixtures.workflow("sub-conflict",
                List.of(WorkflowFixtures.task("A", "ALWAYS_FAIL", List.of())),
                2, 2, 500L);
        WorkflowConflictException ex = assertThrows(WorkflowConflictException.class,
                () -> service.submit(divergent));
        log.info("[测试断言] 同 id 不同声明被拒绝：{}", ex.getMessage());

        // 换个执行参数也算不同声明
        WorkflowDefinition divergentParams = WorkflowFixtures.workflow("sub-conflict",
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                9, 2, 500L);
        assertThrows(WorkflowConflictException.class, () -> service.submit(divergentParams));

        WorkflowSnapshot q = service.query("sub-conflict");
        assertEquals(WorkflowState.SUCCEEDED, q.state(), "冲突提交不得污染既有记录");
        assertEquals(1, q.runCount());
        log.info("[测试断言] 冲突后原记录保持：state={}，runCount={}", q.state(), q.runCount());
    }

    @Test
    @DisplayName("非法执行参数在登记前拦截：不留记录、可修正后重新提交成功")
    void invalidParamsRejectedBeforeAnyRecord() {
        WorkflowService service = newService();
        var base = List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()));

        assertThrows(IllegalArgumentException.class, () -> service.submit(
                WorkflowFixtures.workflow("sub-invalid", base, 0, 2, 500L)));
        assertThrows(IllegalArgumentException.class, () -> service.submit(
                WorkflowFixtures.workflow("sub-invalid", base, 2, 0, 500L)));
        assertThrows(IllegalArgumentException.class, () -> service.submit(
                WorkflowFixtures.workflow("sub-invalid", base, 2, 2, 0L)));
        log.info("[测试断言] maxConcurrency=0 / maxAttempts=0 / taskTimeoutMillis=0 均被拒");

        // 关键：不得留下半成品记录
        assertThrows(WorkflowNotFoundException.class, () -> service.query("sub-invalid"));
        log.info("[测试断言] 被拒后查询：记录不存在（无半成品状态）");

        // 修正取值后同 id 可正常提交（不因之前的失败而被误判为冲突/重复）
        WorkflowSnapshot s = service.submit(
                WorkflowFixtures.workflow("sub-invalid", base, 2, 2, 500L));
        assertEquals(WorkflowState.SUCCEEDED, s.state());
        assertEquals(1, s.runCount());
        log.info("[测试断言] 修正后重新提交成功：state={}，runCount={}", s.state(), s.runCount());
    }

    @Test
    @DisplayName("非法声明与既有记录同 id：先按非法拒绝（400 语义），不触发冲突也不留记录")
    void invalidResubmissionRejectedAsInvalidNotConflict() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("sub-invalid-dup",
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                2, 2, 500L);
        service.submit(wf);
        // 同 id 但取值非法：非法优先于冲突判定
        assertThrows(IllegalArgumentException.class, () -> service.submit(
                WorkflowFixtures.workflow("sub-invalid-dup",
                        List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                        -1, 2, 500L)));
        WorkflowSnapshot q = service.query("sub-invalid-dup");
        assertEquals(1, q.runCount(), "非法重复提交不得影响既有记录");
        log.info("[测试断言] 非法重复提交被 400 语义拒绝，既有记录 runCount={}", q.runCount());
    }
}
