package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowExecution;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowConcurrencyTest.class);

    @Test
    @DisplayName("并发上限=2：4 个独立任务峰值并发恰好为 2，且两两成对时间重叠")
    void concurrencyNeverExceedsLimit() {
        WorkflowEngine engine = EngineTestSupport.newEngine();
        List<com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition> tasks =
                new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            // 每个任务执行 120ms，互不依赖
            tasks.add(WorkflowFixtures.task("T" + i, "ALWAYS_SUCCEED", List.of("120")));
        }
        WorkflowDefinition wf = WorkflowFixtures.workflow("conc2", tasks, 2, 1, 5000L);
        long begin = System.currentTimeMillis();
        WorkflowExecution execution = engine.register(wf);
        engine.execute(execution);
        long elapsed = System.currentTimeMillis() - begin;

        WorkflowSnapshot s = engine.snapshot("conc2").orElseThrow();
        log.info("[测试断言] 峰值并发={}（必须恰好=2，不得超过上限），总耗时={}ms（4*120/2 约 240ms）",
                s.peakConcurrency(), elapsed);
        assertEquals(WorkflowState.SUCCEEDED, s.state());
        assertEquals(2, s.peakConcurrency(), "峰值并发必须恰好等于上限 2");
        // 至少应分两批：总耗时应明显小于串行 480ms，且不小于一批 120ms
        assertTrue(elapsed >= 120 && elapsed < 430,
                "并发执行耗时应介于 120~430ms，实际=" + elapsed);

        Map<String, TaskSnapshot> byName = s.tasks().stream()
                .collect(Collectors.toMap(TaskSnapshot::name, t -> t));
        long overlaps = countOverlappingPairs(byName);
        log.info("[测试断言] 时间窗重叠任务对数={}（>0 证明确实并发，而非串行）", overlaps);
        assertTrue(overlaps >= 2, "至少应有两对任务时间窗重叠，实际=" + overlaps);
    }

    @Test
    @DisplayName("并发上限=1：独立任务串行，峰值并发=1，耗时不小于总时长")
    void concurrencyOneIsSerial() {
        WorkflowEngine engine = EngineTestSupport.newEngine();
        List<com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition> tasks =
                new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            tasks.add(WorkflowFixtures.task("S" + i, "ALWAYS_SUCCEED", List.of("60")));
        }
        WorkflowDefinition wf = WorkflowFixtures.workflow("conc1", tasks, 1, 1, 5000L);
        WorkflowExecution execution = engine.register(wf);
        engine.execute(execution);
        WorkflowSnapshot s = engine.snapshot("conc1").orElseThrow();
        log.info("[测试断言] 峰值并发={}（必须=1），三个任务严格串行", s.peakConcurrency());
        assertEquals(1, s.peakConcurrency());
        assertEquals(WorkflowState.SUCCEEDED, s.state());
    }

    private long countOverlappingPairs(Map<String, TaskSnapshot> tasks) {
        List<String> names = new ArrayList<>(tasks.keySet());
        long pairs = 0;
        for (int i = 0; i < names.size(); i++) {
            for (int j = i + 1; j < names.size(); j++) {
                if (overlaps(tasks.get(names.get(i)), tasks.get(names.get(j)))) {
                    pairs++;
                }
            }
        }
        return pairs;
    }

    /** 快照只含 duration，无法直接取时间窗；这里用峰值并发已足够，占位返回 0 的情况不会发生 */
    private boolean overlaps(TaskSnapshot a, TaskSnapshot b) {
        long aStart = a.startedAtEpochMillis();
        long aEnd = Math.max(a.finishedAtEpochMillis(), aStart + 1);
        long bStart = b.startedAtEpochMillis();
        long bEnd = Math.max(b.finishedAtEpochMillis(), bStart + 1);
        return aStart < bEnd && bStart < aEnd;
    }
}
