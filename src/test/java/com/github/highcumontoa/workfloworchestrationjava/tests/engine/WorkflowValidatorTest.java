package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.RejectReason;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.validation.ValidationException;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkflowValidatorTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowValidatorTest.class);

    private final WorkflowValidator validator = new WorkflowValidator();

    @Test
    @DisplayName("同名任务重复声明 -> DUPLICATE_TASK")
    void rejectsDuplicateTask() {
        WorkflowDefinition wf = WorkflowFixtures.workflow("dup",
                List.of(WorkflowFixtures.task("A"), WorkflowFixtures.task("A")),
                null, null, null);
        ValidationException ex = assertThrows(ValidationException.class,
                () -> validator.validate(wf));
        log.info("[测试断言] 拒绝原因={}，依据=同名任务 A 出现两次，message={}",
                ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.DUPLICATE_TASK, ex.getReason());
    }

    @Test
    @DisplayName("引用不存在的任务 -> UNKNOWN_DEPENDENCY")
    void rejectsUnknownDependency() {
        TaskDefinition a = new TaskDefinition("A", Set.of("GHOST"), "ALWAYS_SUCCEED", List.of());
        WorkflowDefinition wf = WorkflowFixtures.workflow("unknown",
                List.of(WorkflowFixtures.task("B"), a), null, null, null);
        ValidationException ex = assertThrows(ValidationException.class,
                () -> validator.validate(wf));
        log.info("[测试断言] 拒绝原因={}，依据=A 依赖了未声明的 GHOST，message={}",
                ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.UNKNOWN_DEPENDENCY, ex.getReason());
    }

    @Test
    @DisplayName("循环依赖 A->B->C->A -> CYCLIC_DEPENDENCY")
    void rejectsCycle() {
        TaskDefinition a = new TaskDefinition("A", Set.of("C"), "ALWAYS_SUCCEED", List.of());
        TaskDefinition b = new TaskDefinition("B", Set.of("A"), "ALWAYS_SUCCEED", List.of());
        TaskDefinition c = new TaskDefinition("C", Set.of("B"), "ALWAYS_SUCCEED", List.of());
        WorkflowDefinition wf = WorkflowFixtures.workflow("cycle",
                List.of(a, b, c), null, null, null);
        ValidationException ex = assertThrows(ValidationException.class,
                () -> validator.validate(wf));
        log.info("[测试断言] 拒绝原因={}，依据=依赖边构成 A->B->C->A 环，message={}",
                ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.CYCLIC_DEPENDENCY, ex.getReason());
    }

    @Test
    @DisplayName("自依赖 -> CYCLIC_DEPENDENCY")
    void rejectsSelfCycle() {
        TaskDefinition a = new TaskDefinition("A", Set.of("A"), "ALWAYS_SUCCEED", List.of());
        WorkflowDefinition wf = WorkflowFixtures.workflow("self", List.of(a),
                null, null, null);
        ValidationException ex = assertThrows(ValidationException.class,
                () -> validator.validate(wf));
        assertEquals(RejectReason.CYCLIC_DEPENDENCY, ex.getReason());
    }

    @Test
    @DisplayName("合法 DAG（菱形依赖）校验通过")
    void acceptsValidDag() {
        TaskDefinition a = WorkflowFixtures.task("A");
        TaskDefinition b = WorkflowFixtures.task("B", "A");
        TaskDefinition c = WorkflowFixtures.task("C", "A");
        TaskDefinition d = WorkflowFixtures.task("D", "B", "C");
        WorkflowDefinition wf = WorkflowFixtures.workflow("diamond",
                List.of(a, b, c, d), null, null, null);
        assertDoesNotThrow(() -> validator.validate(wf));
        log.info("[测试断言] 菱形 DAG 校验通过，拒绝原因为空");
    }
}
