package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowNotFoundException;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowCancelTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowCancelTest.class);

    private WorkflowService newService() {
        return new WorkflowService(EngineTestSupport.newEngine());
    }

    @Test
    @DisplayName("执行中取消：在途任务收敛并保留结果，未启动任务 CANCELLED 不再执行，工作流落 CANCELLED 终态")
    void cancelRunningWorkflowConvergesSafely() {
        WorkflowService service = newService();
        // A 慢任务（200ms），B/C 依赖链在后；取消时只有 A 在途
        WorkflowDefinition wf = WorkflowFixtures.workflow("cancel-running",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of("200")),
                        WorkflowFixtures.task("B", "ALWAYS_SUCCEED", List.of(), "A"),
                        WorkflowFixtures.task("C", "ALWAYS_SUCCEED", List.of(), "B")
                ), 2, 2, 2000L);

        CompletableFuture<WorkflowSnapshot> submitted =
                CompletableFuture.supplyAsync(() -> service.submit(wf));
        sleep(60); // 等 A 进入在途
        WorkflowSnapshot cancelled = service.cancel("cancel-running");
        WorkflowSnapshot s = submitted.join();
        Map<String, TaskSnapshot> t = byName(s);
        log.info("[测试断言] 取消后：工作流={}，A={}/attempts={}（在途已收敛、结果保留），"
                        + "B={}，C={}（未启动不再执行），runCount={}",
                s.state(), t.get("A").state(), t.get("A").attempts(),
                t.get("B").state(), t.get("C").state(), s.runCount());
        assertEquals(WorkflowState.CANCELLED, s.state());
        assertEquals(WorkflowState.CANCELLED, cancelled.state());
        assertEquals(TaskState.SUCCEEDED, t.get("A").state(), "在途任务应安全收敛并保留成功结果");
        assertEquals(1, t.get("A").attempts());
        assertEquals(TaskState.CANCELLED, t.get("B").state(), "未启动任务不得再开始");
        assertEquals(TaskState.CANCELLED, t.get("C").state());
        assertEquals(0, t.get("B").attempts(), "未启动任务不得有尝试计数");
        assertEquals(0, t.get("C").attempts());
        assertEquals(1, s.runCount());
        assertTrue(s.peakConcurrency() <= 2, "取消过程不得突破并发上限");
    }

    @Test
    @DisplayName("取消幂等且为稳定终态：重复取消无额外影响，恢复请求无法将其拉起")
    void cancelIsIdempotentAndTerminal() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("cancel-idem",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of("200")),
                        WorkflowFixtures.task("B", "ALWAYS_SUCCEED", List.of(), "A")
                ), 2, 2, 2000L);

        CompletableFuture<WorkflowSnapshot> submitted =
                CompletableFuture.supplyAsync(() -> service.submit(wf));
        sleep(60);
        WorkflowSnapshot c1 = service.cancel("cancel-idem");
        submitted.join();
        WorkflowSnapshot c2 = service.cancel("cancel-idem");
        log.info("[测试断言] 重复取消：第一次={}/runCount={}，第二次={}/runCount={}",
                c1.state(), c1.runCount(), c2.state(), c2.runCount());
        assertEquals(WorkflowState.CANCELLED, c1.state());
        assertEquals(WorkflowState.CANCELLED, c2.state());
        assertEquals(c1.runCount(), c2.runCount(), "重复取消不得产生新的执行轮");

        WorkflowSnapshot r = service.resume("cancel-idem");
        log.info("[测试断言] 取消后发起恢复：状态={}，runCount={}（终态不可被拉起）",
                r.state(), r.runCount());
        assertEquals(WorkflowState.CANCELLED, r.state(), "CANCELLED 是稳定终态，恢复不得生效");
        assertEquals(c1.runCount(), r.runCount(), "恢复请求不得增加执行轮次");
    }

    @Test
    @DisplayName("取消 FAILED 工作流：FAILED -> CANCELLED，之后恢复不生效；取消 SUCCEEDED 为幂等空操作")
    void cancelTerminalWorkflows() {
        WorkflowService service = newService();
        WorkflowDefinition failing = WorkflowFixtures.workflow("cancel-failed",
                List.of(WorkflowFixtures.task("A", "ALWAYS_FAIL", List.of())),
                1, 1, 500L);
        WorkflowSnapshot f = service.submit(failing);
        assertEquals(WorkflowState.FAILED, f.state());

        WorkflowSnapshot c = service.cancel("cancel-failed");
        log.info("[测试断言] FAILED 被取消：状态={}，runCount={}", c.state(), c.runCount());
        assertEquals(WorkflowState.CANCELLED, c.state());
        WorkflowSnapshot r = service.resume("cancel-failed");
        assertEquals(WorkflowState.CANCELLED, r.state(), "取消后恢复不得重新拉起");
        assertEquals(c.runCount(), r.runCount());

        WorkflowDefinition ok = WorkflowFixtures.workflow("cancel-succeeded",
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                1, 1, 500L);
        WorkflowSnapshot done = service.submit(ok);
        assertEquals(WorkflowState.SUCCEEDED, done.state());
        WorkflowSnapshot c2 = service.cancel("cancel-succeeded");
        log.info("[测试断言] SUCCEEDED 被取消：状态={}（结论不可撤销，幂等空操作）", c2.state());
        assertEquals(WorkflowState.SUCCEEDED, c2.state(), "已成功工作流的取消不得改变结论");
    }

    @Test
    @DisplayName("取消未登记的工作流：404 语义（WorkflowNotFoundException）")
    void cancelUnknownWorkflowThrowsNotFound() {
        WorkflowService service = newService();
        assertThrows(WorkflowNotFoundException.class, () -> service.cancel("ghost"));
        log.info("[测试断言] 未登记工作流的取消请求被按未找到拒绝");
    }

    private Map<String, TaskSnapshot> byName(WorkflowSnapshot s) {
        return s.tasks().stream().collect(Collectors.toMap(TaskSnapshot::name, x -> x));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
