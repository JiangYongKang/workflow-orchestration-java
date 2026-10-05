package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.config.WorkflowProperties;
import com.github.highcumontoa.workfloworchestrationjava.engine.DeterministicTaskHandler;
import com.github.highcumontoa.workfloworchestrationjava.engine.TaskExecutor;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowNotFoundException;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowCancelTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowCancelTest.class);

    private WorkflowService newService() {
        WorkflowProperties props = new WorkflowProperties();
        props.setRetryBackoffMillis(0);
        props.setTaskTimeoutMillis(5000);
        props.setMaxAttempts(2);
        props.setMaxConcurrency(2);
        TaskExecutor executor = new TaskExecutor(new DeterministicTaskHandler(), props);
        return new WorkflowService(new WorkflowEngine(new WorkflowValidator(), executor, props));
    }

    /** 轮询直到任务进入期望状态（超时 10s 直接失败，避免测试悬死）。 */
    private void waitTaskState(WorkflowService service, String workflowId, String task,
                               TaskState expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                WorkflowSnapshot s = service.query(workflowId);
                TaskState actual = s.tasks().stream()
                        .filter(t -> t.name().equals(task)).findFirst()
                        .map(TaskSnapshot::state).orElse(null);
                if (actual == expected) {
                    return;
                }
            } catch (WorkflowNotFoundException notYetRegistered) {
                // 提交线程尚未完成登记，继续等待
            }
            Thread.sleep(20);
        }
        throw new AssertionError("等待任务 " + task + " 进入 " + expected + " 超时");
    }

    private Map<String, TaskSnapshot> byName(WorkflowSnapshot s) {
        return s.tasks().stream().collect(Collectors.toMap(TaskSnapshot::name, x -> x));
    }

    @Test
    @DisplayName("执行中取消：在途任务安全收敛且结果保留，未启动任务不再开始，取消幂等，恢复不能拉起")
    void cancelRunningWorkflowConvergesSafely() throws Exception {
        WorkflowService service = newService();
        // A 慢任务（2s 窗口供取消到达），B 依赖 A 尚未启动
        WorkflowDefinition wf = WorkflowFixtures.workflow("cancel-running",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of("2000")),
                        WorkflowFixtures.task("B", "ALWAYS_SUCCEED", List.of(), "A")
                ), 2, 2, 5000L);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<WorkflowSnapshot> submitted = pool.submit(() -> service.submit(wf));
        waitTaskState(service, "cancel-running", "A", TaskState.RUNNING);
        log.info("[测试断言] 触发原因=A 已进入 RUNNING，此时发起取消");

        WorkflowSnapshot cancelled = service.cancel("cancel-running");
        WorkflowSnapshot submitView = submitted.get(15, TimeUnit.SECONDS);
        pool.shutdown();
        Map<String, TaskSnapshot> tasks = byName(cancelled);
        log.info("[测试断言] 取消后结论={}：A={}/attempts={}（在途收敛，结果保留），"
                        + "B={}/attempts={}（未启动不再开始），runCount={}，峰值并发={}",
                cancelled.state(), tasks.get("A").state(), tasks.get("A").attempts(),
                tasks.get("B").state(), tasks.get("B").attempts(),
                cancelled.runCount(), cancelled.peakConcurrency());
        assertEquals(WorkflowState.CANCELLED, cancelled.state());
        assertEquals(WorkflowState.CANCELLED, submitView.state(), "提交方拿到的必须是同一份取消结论");
        assertEquals(TaskState.SUCCEEDED, tasks.get("A").state(), "在途任务安全收敛，结果保留");
        assertEquals(1, tasks.get("A").attempts());
        assertEquals(TaskState.CANCELLED, tasks.get("B").state(), "未启动任务不得再开始");
        assertEquals(0, tasks.get("B").attempts());
        assertEquals(1, cancelled.runCount());
        assertTrue(cancelled.peakConcurrency() <= 2, "取消过程并发不得超上限");

        // 重复取消：幂等，无额外影响
        WorkflowSnapshot again = service.cancel("cancel-running");
        log.info("[测试断言] 重复取消：state={}，runCount={}（必须仍为1）",
                again.state(), again.runCount());
        assertEquals(WorkflowState.CANCELLED, again.state());
        assertEquals(1, again.runCount());

        // 取消是稳定终态：恢复请求不能把它拉起来
        WorkflowSnapshot resumed = service.resume("cancel-running");
        log.info("[测试断言] 取消后发起恢复：state={}，runCount={}（必须仍为1，不得重新执行）",
                resumed.state(), resumed.runCount());
        assertEquals(WorkflowState.CANCELLED, resumed.state());
        assertEquals(1, resumed.runCount());
    }

    @Test
    @DisplayName("失败终态上取消：FAILED -> CANCELLED，之后恢复请求不再生效")
    void cancelFailedWorkflowPreventsResume() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("cancel-failed",
                List.of(WorkflowFixtures.task("A", "ALWAYS_FAIL", List.of())),
                1, 1, 1000L);
        WorkflowSnapshot failed = service.submit(wf);
        assertEquals(WorkflowState.FAILED, failed.state());

        WorkflowSnapshot cancelled = service.cancel("cancel-failed");
        log.info("[测试断言] FAILED 上取消：state={}，runCount={}", cancelled.state(),
                cancelled.runCount());
        assertEquals(WorkflowState.CANCELLED, cancelled.state());
        assertEquals(1, cancelled.runCount());

        WorkflowSnapshot resumed = service.resume("cancel-failed");
        log.info("[测试断言] 取消后恢复被幂等忽略：state={}，runCount={}",
                resumed.state(), resumed.runCount());
        assertEquals(WorkflowState.CANCELLED, resumed.state());
        assertEquals(1, resumed.runCount(), "取消后恢复不得产生新一轮执行");
    }

    @Test
    @DisplayName("成功终态上取消：幂等忽略，结论保持 SUCCEEDED；取消未知 id 返回未找到")
    void cancelSucceededWorkflowIsIgnoredAndUnknownIdFails() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("cancel-succeeded",
                List.of(WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of())),
                1, 1, 1000L);
        assertEquals(WorkflowState.SUCCEEDED, service.submit(wf).state());

        WorkflowSnapshot cancelled = service.cancel("cancel-succeeded");
        log.info("[测试断言] SUCCEEDED 上取消：state={}（成功终态不可取消）", cancelled.state());
        assertEquals(WorkflowState.SUCCEEDED, cancelled.state());
        assertEquals(1, cancelled.runCount());

        assertThrows(WorkflowNotFoundException.class, () -> service.cancel("ghost"));
        log.info("[测试断言] 取消未登记 id：按未找到拒绝");
    }
}
