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
        // 第 1 轮里真正跑过的任务，起止时间必须落在第 1 轮的时间窗口内
        assertWithinWindow(r1t.get("A"), r1, "第1轮 A");
        assertWithinWindow(r1t.get("B"), r1, "第1轮 B");
        // 第 1 轮里 C 被跳过、未执行：不得留下任何时间戳
        assertNeverRan(r1t.get("C"), "第1轮 C（被跳过）");

        // 第 2 轮：SUCCEEDED。A 本轮没有重跑：0 次尝试、起止时间与耗时均为 0，
        // 不得沿用第 1 轮的时间戳，也不能把旧时间算进本轮耗时
        assertEquals(2, r2.runNumber());
        assertEquals(WorkflowState.SUCCEEDED, r2.outcome());
        assertEquals(0, r2t.get("A").attempts(), "恢复轮中未重跑的已完成任务必须记 0 次尝试");
        assertNeverRan(r2t.get("A"), "第2轮 A（跨轮保留）");
        assertEquals(TaskState.SUCCEEDED, r2t.get("B").finalState());
        assertEquals(1, r2t.get("B").attempts());
        assertEquals(TaskState.SUCCEEDED, r2t.get("C").finalState());
        assertEquals(1, r2t.get("C").attempts());
        // 第 2 轮里真正重跑的任务，起止时间必须落在第 2 轮的时间窗口内
        assertWithinWindow(r2t.get("B"), r2, "第2轮 B");
        assertWithinWindow(r2t.get("C"), r2, "第2轮 C");

        // 逐轮尝试次数累加 == 任务真实执行总次数，同一次尝试不得在两轮里各算一遍
        assertEquals(1, r1t.get("A").attempts() + r2t.get("A").attempts(),
                "A 实际只跑过 1 次，两轮累加必须等于 1");
        assertEquals(3, r1t.get("B").attempts() + r2t.get("B").attempts(),
                "B 实际跑过 3 次（首轮 2 次重试耗尽 + 恢复轮 1 次），两轮累加必须等于 3");
        assertEquals(1, r1t.get("C").attempts() + r2t.get("C").attempts(),
                "C 实际只跑过 1 次，两轮累加必须等于 1");
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
    @DisplayName("连续三轮恢复：每轮结论与起止限定在各自窗口，逐轮尝试累加等于真实总次数")
    void multiRoundResumeKeepsPerRoundAccounting() {
        WorkflowService service = newService();
        // maxAttempts=1：每轮 B 只尝试 1 次即失败；FAIL_TOTAL_N 预算 2 覆盖前两轮，
        // 第三轮第一次尝试成功。A 首轮成功后跨轮保留，C 前两轮被跳过、第三轮才执行。
        WorkflowDefinition wf = WorkflowFixtures.workflow("hist-multi",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "FAIL_TOTAL_N", List.of("2", "0")),
                        WorkflowFixtures.task("C", "ALWAYS_SUCCEED", List.of(), "B")
                ), 4, 1, 1000L);

        WorkflowSnapshot s1 = service.submit(wf);
        assertEquals(WorkflowState.FAILED, s1.state());
        WorkflowSnapshot s2 = service.resume("hist-multi");
        assertEquals(WorkflowState.FAILED, s2.state());
        WorkflowSnapshot s3 = service.resume("hist-multi");
        assertEquals(WorkflowState.SUCCEEDED, s3.state());
        assertEquals(3, s3.runCount());

        List<RunRecord> history = s3.history();
        assertEquals(3, history.size(), "三轮执行应各有一条留痕");
        RunRecord r1 = history.get(0);
        RunRecord r2 = history.get(1);
        RunRecord r3 = history.get(2);
        Map<String, TaskRunRecord> r1t = byName(r1);
        Map<String, TaskRunRecord> r2t = byName(r2);
        Map<String, TaskRunRecord> r3t = byName(r3);
        for (RunRecord r : history) {
            Map<String, TaskRunRecord> rt = byName(r);
            log.info("[测试断言] 第{}轮：结论={}，窗口=[{}, {}]，A={}/{}次，B={}/{}次，C={}/{}次",
                    r.runNumber(), r.outcome(), r.startedAtEpochMillis(),
                    r.finishedAtEpochMillis(),
                    rt.get("A").finalState(), rt.get("A").attempts(),
                    rt.get("B").finalState(), rt.get("B").attempts(),
                    rt.get("C").finalState(), rt.get("C").attempts());
        }

        // 轮次顺序与结论稳定：FAILED、FAILED、SUCCEEDED
        assertEquals(List.of(1, 2, 3),
                history.stream().map(RunRecord::runNumber).toList());
        assertEquals(WorkflowState.FAILED, r1.outcome());
        assertEquals(WorkflowState.FAILED, r2.outcome());
        assertEquals(WorkflowState.SUCCEEDED, r3.outcome());
        assertEquals("B", r1.failureTask());
        assertEquals("B", r2.failureTask());

        // 各轮时间窗口互不交叠且单调递增：每轮结论限定在自己的窗口内
        assertTrue(r1.finishedAtEpochMillis() >= r1.startedAtEpochMillis());
        assertTrue(r2.startedAtEpochMillis() >= r1.finishedAtEpochMillis(),
                "第2轮不得早于第1轮结束");
        assertTrue(r2.finishedAtEpochMillis() >= r2.startedAtEpochMillis());
        assertTrue(r3.startedAtEpochMillis() >= r2.finishedAtEpochMillis(),
                "第3轮不得早于第2轮结束");
        assertTrue(r3.finishedAtEpochMillis() >= r3.startedAtEpochMillis());

        // 第 1 轮：A 跑过，B 跑过且失败，C 被跳过未执行
        assertWithinWindow(r1t.get("A"), r1, "第1轮 A");
        assertWithinWindow(r1t.get("B"), r1, "第1轮 B");
        assertEquals(TaskState.SKIPPED, r1t.get("C").finalState());
        assertNeverRan(r1t.get("C"), "第1轮 C（被跳过）");

        // 第 2 轮：A 跨轮保留不重跑，B 重跑仍失败，C 仍被跳过
        assertEquals(TaskState.SUCCEEDED, r2t.get("A").finalState());
        assertNeverRan(r2t.get("A"), "第2轮 A（跨轮保留）");
        assertEquals(TaskState.FAILED, r2t.get("B").finalState());
        assertWithinWindow(r2t.get("B"), r2, "第2轮 B");
        assertEquals(TaskState.SKIPPED, r2t.get("C").finalState());
        assertNeverRan(r2t.get("C"), "第2轮 C（被跳过）");

        // 第 3 轮：A 仍跨轮保留，B 重跑成功，C 首次执行成功
        assertNeverRan(r3t.get("A"), "第3轮 A（跨轮保留）");
        assertWithinWindow(r3t.get("B"), r3, "第3轮 B");
        assertWithinWindow(r3t.get("C"), r3, "第3轮 C");

        // 逐轮尝试次数累加 == 任务真实执行总次数
        assertEquals(1, r1t.get("A").attempts() + r2t.get("A").attempts()
                + r3t.get("A").attempts(), "A 全程只真实执行过 1 次");
        assertEquals(3, r1t.get("B").attempts() + r2t.get("B").attempts()
                + r3t.get("B").attempts(), "B 每轮各真实执行 1 次，共 3 次");
        assertEquals(1, r1t.get("C").attempts() + r2t.get("C").attempts()
                + r3t.get("C").attempts(), "C 仅第3轮真实执行 1 次");

        // 反复查询同一份历史：轮次顺序与每轮内容完全一致
        WorkflowSnapshot again = service.query("hist-multi");
        assertEquals(history, again.history(), "反复查询历史必须完全一致、可复现");
    }

    @Test
    @DisplayName("取消终态不可恢复：FAILED 被取消后落 CANCELLED，恢复请求不再拉起也不追加历史")
    void cancelledTerminalStateCannotBeResumed() {
        WorkflowService service = newService();
        WorkflowDefinition wf = WorkflowFixtures.workflow("hist-terminal",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_FAIL", List.of())
                ), 1, 1, 1000L);

        WorkflowSnapshot s1 = service.submit(wf);
        assertEquals(WorkflowState.FAILED, s1.state());
        assertEquals(1, s1.history().size());

        WorkflowSnapshot cancelled = service.cancel("hist-terminal");
        assertEquals(WorkflowState.CANCELLED, cancelled.state());
        assertEquals(1, cancelled.history().size(),
                "FAILED -> CANCELLED 是状态流转而非新一轮执行，不得追加留痕");
        assertEquals(1, cancelled.runCount());

        WorkflowSnapshot resumed = service.resume("hist-terminal");
        log.info("[测试断言] 取消终态收到恢复请求：状态={}，runCount={}，历史轮数={}",
                resumed.state(), resumed.runCount(), resumed.history().size());
        assertEquals(WorkflowState.CANCELLED, resumed.state(),
                "CANCELLED 为稳定终态，恢复请求不得把它重新拉起");
        assertEquals(1, resumed.runCount(), "恢复请求不得触发新一轮执行");
        assertEquals(1, resumed.history().size(), "恢复请求不得追加新轮次");
        assertEquals(cancelled.history(), resumed.history(),
                "既有一轮留痕不得被恢复请求改动");
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

    /** 本轮真实执行过的任务：起止时间必须落在该轮的时间窗口内，耗时为窗口内差值。 */
    private void assertWithinWindow(TaskRunRecord t, RunRecord round, String label) {
        log.info("[测试断言] {}：本轮起={}，止={}，耗时={}ms，轮窗口=[{}, {}]",
                label, t.startedAtEpochMillis(), t.finishedAtEpochMillis(),
                t.durationMillis(), round.startedAtEpochMillis(),
                round.finishedAtEpochMillis());
        assertTrue(t.attempts() > 0, label + "：本轮应至少尝试 1 次");
        assertTrue(t.startedAtEpochMillis() >= round.startedAtEpochMillis(),
                label + "：开始时间不得早于本轮开始");
        assertTrue(t.finishedAtEpochMillis() <= round.finishedAtEpochMillis(),
                label + "：结束时间不得晚于本轮结束");
        assertTrue(t.finishedAtEpochMillis() >= t.startedAtEpochMillis(),
                label + "：结束时间不得早于开始时间");
        assertEquals(t.finishedAtEpochMillis() - t.startedAtEpochMillis(),
                t.durationMillis(), label + "：耗时必须等于本轮起止之差");
    }

    /** 本轮没有执行的任务（跨轮保留/被跳过/被取消）：0 次尝试且不带任何时间戳。 */
    private void assertNeverRan(TaskRunRecord t, String label) {
        log.info("[测试断言] {}：本轮未执行，尝试={}次，起={}，止={}，耗时={}ms",
                label, t.attempts(), t.startedAtEpochMillis(),
                t.finishedAtEpochMillis(), t.durationMillis());
        assertEquals(0, t.attempts(), label + "：本轮未执行，尝试次数必须为 0");
        assertEquals(0, t.startedAtEpochMillis(), label + "：本轮未执行，不得沿用旧开始时间");
        assertEquals(0, t.finishedAtEpochMillis(), label + "：本轮未执行，不得沿用旧结束时间");
        assertEquals(0, t.durationMillis(), label + "：本轮未执行，耗时必须为 0");
    }
}
