package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowExecution;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowDependencyOrderTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDependencyOrderTest.class);

    @Test
    @DisplayName("菱形依赖：A 先于 B、C；B、C 均完成后 D 才执行；B、C 允许并发")
    void executionOrderRespectsDependencies() {
        WorkflowEngine engine = EngineTestSupport.newEngine();
        WorkflowDefinition wf = WorkflowFixtures.workflow("diamond-run",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of("80")),
                        WorkflowFixtures.task("B", "ALWAYS_SUCCEED", List.of("80"), "A"),
                        WorkflowFixtures.task("C", "ALWAYS_SUCCEED", List.of("80"), "A"),
                        WorkflowFixtures.task("D", "ALWAYS_SUCCEED", List.of("0"), "B", "C")
                ), 4, 1, 5000L);

        WorkflowExecution execution = engine.register(wf);
        engine.execute(execution);
        WorkflowSnapshot s = engine.snapshot("diamond-run").orElseThrow();
        Map<String, TaskSnapshot> t = s.tasks().stream()
                .collect(Collectors.toMap(TaskSnapshot::name, x -> x));

        log.info("[测试断言] 状态：A={} B={} C={} D={}，工作流={}",
                t.get("A").state(), t.get("B").state(), t.get("C").state(),
                t.get("D").state(), s.state());
        assertEquals(WorkflowState.SUCCEEDED, s.state());
        t.values().forEach(x -> assertEquals(TaskState.SUCCEEDED, x.state()));

        // 依赖顺序：以完成时间的严格先后来判定
        assertFinishedBefore(t.get("A"), t.get("B"), "A 必须先于 B");
        assertFinishedBefore(t.get("A"), t.get("C"), "A 必须先于 C");
        assertFinishedBefore(t.get("B"), t.get("D"), "B 必须先于 D");
        assertFinishedBefore(t.get("C"), t.get("D"), "C 必须先于 D");
        log.info("[测试断言] 依据=各任务完成时间：A({}) < B({}), C({}) < D({})",
                t.get("A").finishedAtEpochMillis(),
                t.get("B").finishedAtEpochMillis(), t.get("C").finishedAtEpochMillis(),
                t.get("D").finishedAtEpochMillis());

        // B、C 互不依赖，其执行时间窗应允许重叠（这里实际必然重叠）
        boolean bcOverlap = t.get("B").startedAtEpochMillis() < t.get("C").finishedAtEpochMillis()
                && t.get("C").startedAtEpochMillis() < t.get("B").finishedAtEpochMillis();
        log.info("[测试断言] B/C 时间窗重叠={}（互不依赖应可并发）", bcOverlap);
        assertTrue(bcOverlap, "B 与 C 互不依赖，应并发执行");
    }

    private void assertFinishedBefore(TaskSnapshot first, TaskSnapshot then, String msg) {
        assertTrue(first.finishedAtEpochMillis() <= then.finishedAtEpochMillis(),
                msg + "：" + first.name() + "(" + first.finishedAtEpochMillis()
                        + ") 应不晚于 " + then.name() + "("
                        + then.finishedAtEpochMillis() + ")");
    }
}
