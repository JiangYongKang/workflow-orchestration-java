package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.RunRecord;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskRunRecord;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowHistoryTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowHistoryTest.class);

    private WorkflowService newService() {
        return new WorkflowService(EngineTestSupport.newEngine());
    }

    @Test
    @DisplayName("失败再恢复：历史追加两轮，各轮结论/尝试次数/耗时独立留痕，前轮不被覆盖")
    void resumeAppendsRunRecordWithoutOverwriting() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("hist-resume",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "FAIL_TOTAL_N", List.of("2", "0")),
                        WorkflowFixtures.task("C", "ALWAYS_SUCCEED", List.of(), "B")
                ), 4, 2, 1000L);

        service.submit(wf);
        WorkflowSnapshot s2 = service.resume("hist-resume");
        assertEquals(WorkflowState.SUCCEEDED, s2.state());
        assertEquals(2, s2.runCount());

        List<RunRecord> history = s2.history();
        assertEquals(2, history.size(), "两轮执行应各有一条留痕");
        RunRecord r1 = history.get(0);
        RunRecord r2 = history.get(1);
        Map<String, TaskRunRecord> r1t = byName(r1);
        Map<String, TaskRunRecord> r2t = byName(r2);
        log.info("[测试断言] 第1轮：结论={}，A={}/{}次，B={}/{}次，C={}；第2轮：结论={}，"
                        + "A={}/{}次，B={}/{}次，C={}/{}次",
                r1.outcome(), r1t.get("A").finalState(), r1t.get("A").attempts(),
                r1t.get("B").finalState(), r1t.get("B").attempts(),
                r1t.get("C").finalState(),
                r2.outcome(), r2t.get("A").finalState(), r2t.get("A").attempts(),
                r2t.get("B").finalState(), r2t.get("B").attempts(),
                r2t.get("C").finalState(), r2t.get("C").attempts());

        // 第 1 轮：FAILED，B 重试耗尽 2 次，C 被跳过
        assertEquals(1, r1.runNumber());
        assertEquals(WorkflowState.FAILED, r1.outcome());
        assertEquals("B", r1.failureTask());
        assertEquals(TaskState.SUCCEEDED, r1t.get("A").finalState());
        assertEquals(1, r1t.get("A").attempts());
        assertEquals(TaskState.FAILED, r1t.get("B").finalState());
        assertEquals(2, r1t.get("B").attempts());
        assertEquals(TaskState.SKIPPED, r1t.get("C").finalState());
        assertEquals(0, r1t.get("C").attempts());
        assertTrue(r1.finishedAtEpochMillis() >= r1.startedAtEpochMillis());

        // 第 2 轮：SUCCEEDED，A 保持 1 次（不重跑不重复计数），B 重新计数为 1
        assertEquals(2, r2.runNumber());
        assertEquals(WorkflowState.SUCCEEDED, r2.outcome());
        assertEquals(1, r2t.get("A").attempts(), "恢复轮不得对已完成任务重复计数");
        assertEquals(TaskState.SUCCEEDED, r2t.get("B").finalState());
        assertEquals(1, r2t.get("B").attempts());
        assertEquals(TaskState.SUCCEEDED, r2t.get("C").finalState());
        assertEquals(1, r2t.get("C").attempts());
    }

    @Test
    @DisplayName("反复查询历史：结论一致、顺序稳定；恢复只追加不重复计数")
    void historyIsStableAcrossQueries() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("hist-stable",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "FAIL_TOTAL_N", List.of("1", "0"))
                ), 2, 1, 1000L);

        service.submit(wf);
        service.resume("hist-stable");

        WorkflowSnapshot q1 = service.query("hist-stable");
        WorkflowSnapshot q2 = service.query("hist-stable");
        log.info("[测试断言] 两次查询历史：轮次数 {}=={}，逐轮记录相等={}",
                q1.history().size(), q2.history().size(),
                q1.history().equals(q2.history()));
        assertEquals(2, q1.history().size());
        assertEquals(q1.history(), q2.history(), "反复查询同一份历史必须一致");
        assertEquals(List.of(1, 2),
                q1.history().stream().map(RunRecord::runNumber).toList(),
                "轮次顺序必须稳定递增");
        // 每轮任务明细顺序与声明顺序一致，稳定可复现
        assertEquals(List.of("A", "B"),
                q1.history().get(0).tasks().stream().map(TaskRunRecord::name).toList());
        // 再恢复一次（已成功，幂等忽略）不得追加历史
        service.resume("hist-stable");
        assertEquals(2, service.query("hist-stable").history().size(),
                "幂等忽略的恢复不得追加历史记录");
    }

    @Test
    @DisplayName("取消的工作流也留下一轮结论为 CANCELLED 的留痕")
    void cancelledRunIsRecorded() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("hist-cancel",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of("200")),
                        WorkflowFixtures.task("B", "ALWAYS_SUCCEED", List.of(), "A")
                ), 2, 2, 2000L);

        java.util.concurrent.CompletableFuture<WorkflowSnapshot> submitted =
                java.util.concurrent.CompletableFuture.supplyAsync(() -> service.submit(wf));
        try {
            Thread.sleep(60);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        service.cancel("hist-cancel");
        WorkflowSnapshot s = submitted.join();

        assertEquals(WorkflowState.CANCELLED, s.state());
        assertEquals(1, s.history().size());
        RunRecord r = s.history().get(0);
        Map<String, TaskRunRecord> rt = byName(r);
        log.info("[测试断言] 取消轮留痕：结论={}，A={}/{}次，B={}/{}次",
                r.outcome(), rt.get("A").finalState(), rt.get("A").attempts(),
                rt.get("B").finalState(), rt.get("B").attempts());
        assertEquals(WorkflowState.CANCELLED, r.outcome());
        assertEquals(TaskState.SUCCEEDED, rt.get("A").finalState(), "已完成任务结果保留在历史中");
        assertEquals(1, rt.get("A").attempts());
        assertEquals(TaskState.CANCELLED, rt.get("B").finalState());
        assertEquals(0, rt.get("B").attempts());
    }

    private Map<String, TaskRunRecord> byName(RunRecord r) {
        return r.tasks().stream()
                .collect(Collectors.toMap(TaskRunRecord::name, Function.identity()));
    }
}
