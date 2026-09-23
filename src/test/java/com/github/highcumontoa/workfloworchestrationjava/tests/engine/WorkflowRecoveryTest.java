package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskSnapshot;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkflowRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRecoveryTest.class);

    private WorkflowService newService() {
        return new WorkflowService(EngineTestSupport.newEngine());
    }

    @Test
    @DisplayName("失败后恢复成功：FAIL_TOTAL_N(2)+上限2，首轮 FAILED、C 跳过；恢复后 B/C 成功，A 不重跑")
    void resumeContinuesFromCompletedTasksAndSucceeds() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("recover-ok",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "FAIL_TOTAL_N", List.of("2", "0")),
                        WorkflowFixtures.task("C", "ALWAYS_SUCCEED", List.of("0"), "B")
                ), 4, 2, 1000L);

        WorkflowSnapshot s1 = service.submit(wf);
        Map<String, TaskSnapshot> t1 = byName(s1);
        log.info("[测试断言] 首轮结论={}：A={} B={}/attempts={} C={}，失败任务={}",
                s1.state(), t1.get("A").state(), t1.get("B").state(),
                t1.get("B").attempts(), t1.get("C").state(), s1.failureTask());
        assertEquals(WorkflowState.FAILED, s1.state());
        assertEquals(TaskState.SUCCEEDED, t1.get("A").state());
        assertEquals(TaskState.FAILED, t1.get("B").state());
        assertEquals(2, t1.get("B").attempts());
        assertEquals(TaskState.SKIPPED, t1.get("C").state());

        WorkflowSnapshot s2 = service.resume("recover-ok");
        Map<String, TaskSnapshot> t2 = byName(s2);
        log.info("[测试断言] 恢复轮结论={}：A.attempts={}（保持1，不重复执行），"
                        + "B.state={}/attempts={}（新一轮计数），C.state={}/attempts={}，runCount={}",
                s2.state(), t2.get("A").attempts(), t2.get("B").state(),
                t2.get("B").attempts(), t2.get("C").state(), t2.get("C").attempts(),
                s2.runCount());
        assertEquals(WorkflowState.SUCCEEDED, s2.state());
        assertEquals(TaskState.SUCCEEDED, t2.get("A").state());
        assertEquals(1, t2.get("A").attempts(), "已完成任务不得重复计数");
        assertEquals(TaskState.SUCCEEDED, t2.get("B").state());
        assertEquals(1, t2.get("B").attempts(), "B 在新执行轮重新计数");
        assertEquals(TaskState.SUCCEEDED, t2.get("C").state());
        assertEquals(1, t2.get("C").attempts(), "C 在恢复轮才首次执行");
        assertEquals(2, s2.runCount());

        // 恢复成功后再次 resume 必须幂等，不产生新执行
        WorkflowSnapshot s3 = service.resume("recover-ok");
        assertEquals(WorkflowState.SUCCEEDED, s3.state());
        assertEquals(2, s3.runCount(), "成功工作流的恢复请求不得增加执行轮次");
        log.info("[测试断言] 成功后再次 resume 幂等：runCount 仍={}", s3.runCount());

        // 多次查询结论一致
        WorkflowSnapshot q1 = service.query("recover-ok");
        WorkflowSnapshot q2 = service.query("recover-ok");
        assertEquals(q1.state(), q2.state());
        assertEquals(3, q1.tasks().size());
        assertEquals(WorkflowState.SUCCEEDED, q1.state());
        log.info("[测试断言] 多次查询结论稳定：state={}，failureTask={}",
                q1.state(), q1.failureTask());
    }

    @Test
    @DisplayName("恢复仍失败：ALWAYS_FAIL 两轮都 FAILED，A 不重跑，失败原因稳定可解释")
    void resumeThatStillFailsKeepsStableConclusion() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("recover-fail",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "ALWAYS_FAIL", List.of()),
                        WorkflowFixtures.task("C", "ALWAYS_SUCCEED", List.of(), "B")
                ), 4, 2, 1000L);

        WorkflowSnapshot s1 = service.submit(wf);
        assertEquals(WorkflowState.FAILED, s1.state());
        WorkflowSnapshot s2 = service.resume("recover-fail");
        Map<String, TaskSnapshot> t2 = byName(s2);
        log.info("[测试断言] 恢复后仍失败：结论={}，A.attempts={}，B.attempts={}，C={}，原因={}",
                s2.state(), t2.get("A").attempts(), t2.get("B").attempts(),
                t2.get("C").state(), s2.failureReason());
        assertEquals(WorkflowState.FAILED, s2.state());
        assertEquals(1, t2.get("A").attempts());
        assertEquals(2, t2.get("B").attempts());
        assertEquals(TaskState.SKIPPED, t2.get("C").state());
        assertEquals("B", s2.failureTask());
        org.junit.jupiter.api.Assertions.assertTrue(s2.failureReason().contains("ALWAYS_FAIL"));
    }

    private Map<String, TaskSnapshot> byName(WorkflowSnapshot s) {
        return s.tasks().stream().collect(Collectors.toMap(TaskSnapshot::name, x -> x));
    }
}
