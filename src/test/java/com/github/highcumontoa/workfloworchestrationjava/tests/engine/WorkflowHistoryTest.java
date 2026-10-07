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

        // 第 2 轮：SUCCEEDED；A 本轮未重跑，留痕必须是 0 次尝试、起止时间与耗时均为 0，
        // 不得沿用第 1 轮的旧值（否则同一次尝试会在两轮各算一遍）
        assertEquals(2, r2.runNumber());
        assertEquals(WorkflowState.SUCCEEDED, r2.outcome());
        assertEquals(TaskState.SUCCEEDED, r2t.get("A").finalState(),
                "跨轮保留的已完成任务在新一轮仍呈现其结论状态");
        assertEquals(0, r2t.get("A").attempts(),
                "本轮未重跑的任务在本轮必须记 0 次尝试，不得沿用上一轮计数");
        assertEquals(0, r2t.get("A").startedAtEpochMillis(),
                "本轮未重跑的任务不得沿用上一轮的开始时间");
        assertEquals(0, r2t.get("A").finishedAtEpochMillis(),
                "本轮未重跑的任务不得沿用上一轮的结束时间");
        assertEquals(0, r2t.get("A").durationMillis(),
                "本轮未重跑的任务不得把旧时间算成本轮耗时");
        assertEquals(TaskState.SUCCEEDED, r2t.get("B").finalState());
        assertEquals(1, r2t.get("B").attempts());
        assertEquals(TaskState.SUCCEEDED, r2t.get("C").finalState());
        assertEquals(1, r2t.get("C").attempts());

        // 本轮真实执行的任务，其起止时间必须落在本轮的时间窗口内
        for (String name : List.of("B", "C")) {
            TaskRunRecord rec = r2t.get(name);
            assertTrue(rec.startedAtEpochMillis() >= r2.startedAtEpochMillis(),
                    name + " 本轮开始时间不得早于本轮起跑线");
            assertTrue(rec.finishedAtEpochMillis() <= r2.finishedAtEpochMillis(),
                    name + " 本轮结束时间不得晚于本轮收尾");
            assertTrue(rec.durationMillis() >= 0);
        }

        // 逐轮尝试次数之和 == 任务真实执行总次数（A=1、B=3、C=1）
        assertEquals(1, r1t.get("A").attempts() + r2t.get("A").attempts(),
                "A 跨轮尝试累加必须等于真实总次数 1");
        assertEquals(3, r1t.get("B").attempts() + r2t.get("B").attempts(),
                "B 跨轮尝试累加必须等于真实总次数 3（首轮 2 次失败 + 恢复轮 1 次成功）");
        assertEquals(1, r1t.get("C").attempts() + r2t.get("C").attempts(),
                "C 跨轮尝试累加必须等于真实总次数 1");
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
    @DisplayName("三轮连续恢复：每轮结论/起止限定在本轮窗口，跨轮尝试累加等于真实总次数，前轮不被改写")
    void multiRoundRecoveryAccumulatesConsistently() {
        WorkflowService service = newService();
        // B 每轮重试上限 1 次、跨轮失败预算 2：第 1、2 轮各失败 1 次，第 3 轮成功；
        // C 依赖 B，前两轮被跳过，第 3 轮才真实执行
        WorkflowDefinition wf = WorkflowFixtures.workflow("hist-multi",
                List.of(
                        WorkflowFixtures.task("A", "ALWAYS_SUCCEED", List.of()),
                        WorkflowFixtures.task("B", "FAIL_TOTAL_N", List.of("2", "0")),
                        WorkflowFixtures.task("C", "ALWAYS_SUCCEED", List.of(), "B")
                ), 3, 1, 1000L);

        WorkflowSnapshot s1 = service.submit(wf);
        assertEquals(WorkflowState.FAILED, s1.state());
        WorkflowSnapshot s2 = service.resume("hist-multi");
        assertEquals(WorkflowState.FAILED, s2.state());
        // 第 2 轮结束时先留一份历史快照，用于验证第 3 轮恢复绝不回头改写前轮
        List<RunRecord> historyAfterRound2 = s2.history();
        WorkflowSnapshot s3 = service.resume("hist-multi");
        assertEquals(WorkflowState.SUCCEEDED, s3.state());
        assertEquals(3, s3.runCount());

        List<RunRecord> history = s3.history();
        assertEquals(3, history.size(), "三轮执行应各有一条留痕");
        assertEquals(List.of(1, 2, 3),
                history.stream().map(RunRecord::runNumber).toList(), "轮次顺序稳定递增");
        assertEquals(List.of(WorkflowState.FAILED, WorkflowState.FAILED,
                        WorkflowState.SUCCEEDED),
                history.stream().map(RunRecord::outcome).toList());

        // 恢复只追加：第 3 轮之后，前两轮的留痕与第 2 轮结束时读到的一模一样
        assertEquals(historyAfterRound2, history.subList(0, 2),
                "恢复只能追加新记录，绝不能回头改动或覆盖前面任意一轮");

        // 每轮结论与起止都限定在各自轮次的时间窗口内，且窗口按轮次顺序排列
        for (RunRecord r : history) {
            assertTrue(r.finishedAtEpochMillis() >= r.startedAtEpochMillis(),
                    "第" + r.runNumber() + "轮收尾不得早于起跑");
        }
        assertTrue(history.get(0).finishedAtEpochMillis()
                <= history.get(1).startedAtEpochMillis(), "第 2 轮窗口不得早于第 1 轮收尾");
        assertTrue(history.get(1).finishedAtEpochMillis()
                <= history.get(2).startedAtEpochMillis(), "第 3 轮窗口不得早于第 2 轮收尾");

        // 逐轮每任务尝试次数：A=[1,0,0]，B=[1,1,1]，C=[0,0,1]
        int[][] expectedAttempts = {{1, 0, 0}, {1, 1, 1}, {0, 0, 1}};
        String[] names = {"A", "B", "C"};
        for (int i = 0; i < names.length; i++) {
            for (int round = 0; round < 3; round++) {
                TaskRunRecord rec = byName(history.get(round)).get(names[i]);
                log.info("[测试断言] 第{}轮 任务{}：状态={}，本轮尝试={}次，起={}，止={}，耗时={}ms",
                        round + 1, names[i], rec.finalState(), rec.attempts(),
                        rec.startedAtEpochMillis(), rec.finishedAtEpochMillis(),
                        rec.durationMillis());
                assertEquals(expectedAttempts[i][round], rec.attempts(),
                        "第" + (round + 1) + "轮任务" + names[i] + "的尝试次数必须只反映本轮");
            }
        }

        // 跨轮累加 == 真实执行总次数：A=1，B=3，C=1
        for (int i = 0; i < names.length; i++) {
            int sum = 0;
            for (int round = 0; round < 3; round++) {
                sum += byName(history.get(round)).get(names[i]).attempts();
            }
            int expectedTotal = switch (names[i]) {
                case "A" -> 1;
                case "B" -> 3;
                default -> 1;
            };
            assertEquals(expectedTotal, sum,
                    "任务" + names[i] + "跨轮尝试累加必须等于真实执行总次数");
        }

        // 本轮未执行的任务：尝试 0 次、起止与耗时均为 0，且不得被算成执行过
        assertEquals(TaskState.SKIPPED, byName(history.get(0)).get("C").finalState());
        assertEquals(TaskState.SKIPPED, byName(history.get(1)).get("C").finalState());
        for (int round = 0; round < 3; round++) {
            RunRecord r = history.get(round);
            for (TaskRunRecord rec : r.tasks()) {
                if (rec.attempts() == 0) {
                    assertEquals(0, rec.startedAtEpochMillis(),
                            "本轮未执行的任务不得沿用旧开始时间（第" + r.runNumber()
                                    + "轮 " + rec.name() + "）");
                    assertEquals(0, rec.finishedAtEpochMillis(),
                            "本轮未执行的任务不得沿用旧结束时间（第" + r.runNumber()
                                    + "轮 " + rec.name() + "）");
                    assertEquals(0, rec.durationMillis(),
                            "本轮未执行的任务不得把旧时间算成本轮耗时（第" + r.runNumber()
                                    + "轮 " + rec.name() + "）");
                } else {
                    assertTrue(rec.startedAtEpochMillis() >= r.startedAtEpochMillis()
                                    && rec.finishedAtEpochMillis() <= r.finishedAtEpochMillis(),
                            "本轮执行的任务起止必须落在第" + r.runNumber() + "轮窗口内（"
                                    + rec.name() + "）");
                }
            }
        }

        // 已成功终态：恢复请求幂等忽略，不再追加新轮次
        WorkflowSnapshot ignored = service.resume("hist-multi");
        assertEquals(WorkflowState.SUCCEEDED, ignored.state());
        assertEquals(3, service.query("hist-multi").history().size(),
                "SUCCEEDED 终态不得被恢复请求重新拉起，也不得追加新轮次");

        // 反复查询：轮次顺序与每轮内容完全一致、可复现
        assertEquals(history, service.query("hist-multi").history(),
                "反复查询同一份历史必须完全一致");
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
