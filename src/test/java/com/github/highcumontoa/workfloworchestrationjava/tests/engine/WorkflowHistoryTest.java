package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskRunRecord;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowHistory;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowRunRecord;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowNotFoundException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowHistoryTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowHistoryTest.class);

    private WorkflowService newService() {
        return new WorkflowService(EngineTestSupport.newEngine());
    }

    private Map<String, TaskRunRecord> byName(WorkflowRunRecord run) {
        return run.tasks().stream()
                .collect(Collectors.toMap(TaskRunRecord::name, Function.identity()));
    }

    @Test
    @DisplayName("失败恢复产生两轮历史：逐轮结论/任务尝试次数可翻账，反复查询一致，顺序稳定")
    void resumeAppendsRunRecordWithoutOverwriting() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("hist-recover",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "FAIL_TOTAL_N", List.of("2", "0")),
                        WorkflowFixtures.task("C", "ALWAYS_SUCCEED", List.of("0"), "B")
                ), 4, 2, 1000L);

        service.submit(wf);
        service.resume("hist-recover");

        WorkflowHistory h1 = service.history("hist-recover");
        WorkflowHistory h2 = service.history("hist-recover");
        log.info("[测试断言] 反复查询历史：两次结论必须完全一致（record 值相等）");
        assertEquals(h1, h2, "反复查询同一份历史必须一致");

        assertEquals(2, h1.runs().size(), "恢复只追加一条新记录");
        WorkflowRunRecord run1 = h1.runs().get(0);
        WorkflowRunRecord run2 = h1.runs().get(1);
        log.info("[测试断言] 轮次顺序：run1.number={} 结论={}，run2.number={} 结论={}",
                run1.runNumber(), run1.conclusion(), run2.runNumber(), run2.conclusion());
        assertEquals(1, run1.runNumber());
        assertEquals(2, run2.runNumber());
        assertEquals(WorkflowState.FAILED, run1.conclusion());
        assertEquals(WorkflowState.SUCCEEDED, run2.conclusion());

        // 每轮起止时间齐全且顺序稳定：第一轮结束不晚于第二轮开始
        assertTrue(run1.startedAtEpochMillis() > 0 && run1.finishedAtEpochMillis() > 0);
        assertTrue(run1.finishedAtEpochMillis() <= run2.startedAtEpochMillis(),
                "第一轮结束不得晚于第二轮开始");

        // 第一轮逐任务翻账：A 成功1次、B 失败2次（恰好上限）、C 被跳过0次
        Map<String, TaskRunRecord> r1 = byName(run1);
        log.info("[测试断言] 第1轮：A={}/{}次，B={}/{}次，C={}/{}次，失败任务={}",
                r1.get("A").state(), r1.get("A").attempts(),
                r1.get("B").state(), r1.get("B").attempts(),
                r1.get("C").state(), r1.get("C").attempts(), run1.failureTask());
        assertEquals(TaskState.SUCCEEDED, r1.get("A").state());
        assertEquals(1, r1.get("A").attempts());
        assertEquals(TaskState.FAILED, r1.get("B").state());
        assertEquals(2, r1.get("B").attempts());
        assertEquals(TaskState.SKIPPED, r1.get("C").state());
        assertEquals(0, r1.get("C").attempts());
        assertEquals("B", run1.failureTask());

        // 第二轮：A 为上一轮保留（attempts=0，不重复计数），B 重新计1次，C 首次执行
        Map<String, TaskRunRecord> r2 = byName(run2);
        log.info("[测试断言] 第2轮：A={}/{}次（保留不计数），B={}/{}次，C={}/{}次",
                r2.get("A").state(), r2.get("A").attempts(),
                r2.get("B").state(), r2.get("B").attempts(),
                r2.get("C").state(), r2.get("C").attempts());
        assertEquals(TaskState.SUCCEEDED, r2.get("A").state());
        assertEquals(0, r2.get("A").attempts(), "保留任务不得在新一轮重复计数");
        assertEquals(TaskState.SUCCEEDED, r2.get("B").state());
        assertEquals(1, r2.get("B").attempts());
        assertEquals(TaskState.SUCCEEDED, r2.get("C").state());
        assertEquals(1, r2.get("C").attempts());

        // 历史只追加：再次查询仍然是这两条，且内容未被覆盖
        WorkflowHistory h3 = service.history("hist-recover");
        assertEquals(h1, h3);
    }

    @Test
    @DisplayName("单次成功与取消各产生一条历史记录；未知 id 查历史返回未找到")
    void singleRunAndCancelledRunAreRecorded() {
        WorkflowService service = newService();
        WorkflowDefinition ok = WorkflowFixtures.workflow("hist-ok",
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                1, 1, 1000L);
        service.submit(ok);
        WorkflowHistory h = service.history("hist-ok");
        assertEquals(1, h.runs().size());
        WorkflowRunRecord run = h.runs().get(0);
        log.info("[测试断言] 单轮成功：runNumber={}，结论={}，A={}/{}次，耗时={}ms",
                run.runNumber(), run.conclusion(),
                run.tasks().get(0).state(), run.tasks().get(0).attempts(),
                run.tasks().get(0).durationMillis());
        assertEquals(1, run.runNumber());
        assertEquals(WorkflowState.SUCCEEDED, run.conclusion());
        assertEquals(1, run.tasks().get(0).attempts());

        WorkflowDefinition fail = WorkflowFixtures.workflow("hist-cancel",
                List.of(WorkflowFixtures.task("A", "ALWAYS_FAIL", List.of())),
                1, 1, 1000L);
        service.submit(fail);
        service.cancel("hist-cancel");
        WorkflowHistory hc = service.history("hist-cancel");
        log.info("[测试断言] 取消的工作流：历史轮数={}，第1轮结论={}，当前状态={}",
                hc.runs().size(), hc.runs().get(0).conclusion(),
                service.query("hist-cancel").state());
        assertEquals(1, hc.runs().size(), "取消本身不追加执行轮");
        assertEquals(WorkflowState.FAILED, hc.runs().get(0).conclusion());
        assertEquals(WorkflowState.CANCELLED, service.query("hist-cancel").state());

        assertThrows(WorkflowNotFoundException.class, () -> service.history("ghost"));
        log.info("[测试断言] 未登记 id 查历史：按未找到拒绝");
    }
}
