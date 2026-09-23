package com.github.highcumontoa.workfloworchestrationjava.tests;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskAction;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskSpec;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSpec;
import com.github.highcumontoa.workfloworchestrationjava.validation.RejectReason;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidationException;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowValidatorTests {

    private static final Logger log = LoggerFactory.getLogger(WorkflowValidatorTests.class);

    private final WorkflowValidator validator = new WorkflowValidator();

    private TaskSpec task(String id, String... deps) {
        return new TaskSpec(id, List.of(deps), TaskAction.SUCCEED, 0, 0L);
    }

    @Test
    void rejectsDuplicateTaskWithDistinctReason() {
        WorkflowSpec spec = new WorkflowSpec("wf-dup", List.of(task("a"), task("a")));

        WorkflowValidationException ex = assertThrows(WorkflowValidationException.class,
                () -> validator.validateAndOrder(spec));

        log.info("[trigger] duplicate task 'a' -> [state-basis] reject reason={}, detail={}",
                ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.DUPLICATE_TASK, ex.getReason());
    }

    @Test
    void rejectsUnknownDependencyWithDistinctReason() {
        WorkflowSpec spec = new WorkflowSpec("wf-unknown",
                List.of(task("a", "ghost"), task("b", "a")));

        WorkflowValidationException ex = assertThrows(WorkflowValidationException.class,
                () -> validator.validateAndOrder(spec));

        log.info("[trigger] task 'a' depends on missing 'ghost' -> [state-basis] reject reason={}, detail={}",
                ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.UNKNOWN_DEPENDENCY, ex.getReason());
    }

    @Test
    void rejectsCyclicDependencyWithDistinctReason() {
        WorkflowSpec spec = new WorkflowSpec("wf-cycle",
                List.of(task("a", "c"), task("b", "a"), task("c", "b")));

        WorkflowValidationException ex = assertThrows(WorkflowValidationException.class,
                () -> validator.validateAndOrder(spec));

        log.info("[trigger] a->c->b->a forms a cycle -> [state-basis] reject reason={}, detail={}",
                ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.CYCLIC_DEPENDENCY, ex.getReason());
    }

    @Test
    void validWorkflowGetsDependencyRespectingOrder() {
        // 菱形依赖：d 依赖 b、c；b、c 依赖 a
        WorkflowSpec spec = new WorkflowSpec("wf-diamond",
                List.of(task("d", "b", "c"), task("b", "a"), task("a"), task("c", "a")));

        List<String> order = validator.validateAndOrder(spec);

        log.info("[trigger] valid diamond workflow -> [state-basis] computed order={}", order);
        assertEquals(0, order.indexOf("a"), "a has no dependency and must run first");
        assertTrue(order.indexOf("a") < order.indexOf("b"));
        assertTrue(order.indexOf("a") < order.indexOf("c"));
        assertTrue(order.indexOf("b") < order.indexOf("d"));
        assertTrue(order.indexOf("c") < order.indexOf("d"),
                "execution order must satisfy every declared dependency");
    }
}
