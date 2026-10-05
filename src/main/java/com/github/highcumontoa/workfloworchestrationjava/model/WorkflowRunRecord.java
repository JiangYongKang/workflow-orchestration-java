package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;

/** 一轮执行的完整记录（不可变，追加式历史）。 */
public record WorkflowRunRecord(
        int runNumber,
        long startedAtEpochMillis,
        long finishedAtEpochMillis,
        WorkflowState conclusion,
        String failureTask,
        String failureReason,
        List<TaskRunRecord> tasks
) {
}
