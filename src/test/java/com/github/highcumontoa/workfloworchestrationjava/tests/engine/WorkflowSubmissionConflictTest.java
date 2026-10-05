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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowSubmissionConflictTest {

    private static final Logger log =
            LoggerFactory.getLogger(WorkflowSubmissionConflictTest.class);

    private WorkflowService newService() {
        return new WorkflowService(EngineTestSupport.newEngine());
    }

    @Test
    @DisplayName("同 id 不同声明：明确报冲突，既有记录不被污染；声明一致（含顺序/默认值等价）仍幂等")
    void conflictingDefinitionIsRejectedAndIdenticalStaysIdempotent() {
        WorkflowService service = newService();
        WorkflowDefinition original = WorkflowFixtures.workflow("conflict-1",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "ALWAYS_SUCCEED", List.of(), "A")
                ), null, null, null);
        WorkflowSnapshot first = service.submit(original);
        assertEquals(WorkflowState.SUCCEEDED, first.state());

        // 1) 语义等价的声明：任务顺序不同 + 显式默认值（2/3/500 为测试引擎默认）→ 仍幂等
        WorkflowDefinition equivalent = WorkflowFixtures.workflow("conflict-1",
                List.of(
                        WorkflowFixtures.task("B", "ALWAYS_SUCCEED", List.of(), "A"),
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())
                ), 2, 3, 500L);
        WorkflowSnapshot dup = service.submit(equivalent);
        log.info("[测试断言] 语义等价声明（顺序/默认值差异）：duplicate={}，runCount={}",
                dup.duplicateSubmission(), dup.runCount());
        assertTrue(dup.duplicateSubmission());
        assertEquals(1, dup.runCount());

        // 2) 不同声明（B 的行为变了）→ 明确冲突
        WorkflowDefinition changed = WorkflowFixtures.workflow("conflict-1",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "ALWAYS_FAIL", List.of(), "A")
                ), null, null, null);
        WorkflowConflictException conflict = assertThrows(WorkflowConflictException.class,
                () -> service.submit(changed));
        log.info("[测试断言] 同 id 不同声明被拒绝：{}", conflict.getMessage());

        // 3) 冲突不影响既有记录：结论仍是首次执行的 SUCCEEDED，runCount 仍为 1
        WorkflowSnapshot after = service.query("conflict-1");
        log.info("[测试断言] 冲突后既有记录：state={}，runCount={}，duplicate={}",
                after.state(), after.runCount(), after.duplicateSubmission());
        assertEquals(WorkflowState.SUCCEEDED, after.state());
        assertEquals(1, after.runCount());
        assertFalse(after.duplicateSubmission());
    }

    @Test
    @DisplayName("非法执行参数在登记前被挡掉：不留记录，修正后同 id 可正常提交")
    void invalidExecutionParamsRejectedBeforeRegistration() {
        WorkflowService service = newService();
        List<WorkflowDefinition> invalids = List.of(
                WorkflowFixtures.workflow("bad-param", List.of(
                        WorkflowFixtures.task("A")), 0, 1, 1000L),
                WorkflowFixtures.workflow("bad-param", List.of(
                        WorkflowFixtures.task("A")), 1, 0, 1000L),
                WorkflowFixtures.workflow("bad-param", List.of(
                        WorkflowFixtures.task("A")), 1, 1, 0L));
        for (WorkflowDefinition invalid : invalids) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> service.submit(invalid));
            log.info("[测试断言] 非法参数被拒绝（登记前）：{}", ex.getMessage());
            // 每次拒绝后都不允许留下任何记录
            assertThrows(WorkflowNotFoundException.class,
                    () -> service.query("bad-param"));
        }
        log.info("[测试断言] 三种非法取值（并发/重试/超时）均未留下记录");

        // 修正取值后同 id 可正常提交并执行
        WorkflowDefinition fixed = WorkflowFixtures.workflow("bad-param",
                List.of(WorkflowFixtures.task("A")), 1, 1, 1000L);
        WorkflowSnapshot ok = service.submit(fixed);
        log.info("[测试断言] 修正后同 id 提交：state={}，runCount={}", ok.state(), ok.runCount());
        assertEquals(WorkflowState.SUCCEEDED, ok.state());
        assertEquals(1, ok.runCount());
    }
}
