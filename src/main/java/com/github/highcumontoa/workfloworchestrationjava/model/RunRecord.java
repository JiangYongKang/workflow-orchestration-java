package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;

/** 一轮执行的留痕（不可变）：起止时间、结论与该轮各任务明细。 */
public record RunRecord(
        int runNumber,
        long startedAtEpochMillis,
        long finishedAtEpochMillis,
        WorkflowState outcome,
        String failureTask,
        String failureReason,
        List<TaskRunRecord> tasks
) {
}
