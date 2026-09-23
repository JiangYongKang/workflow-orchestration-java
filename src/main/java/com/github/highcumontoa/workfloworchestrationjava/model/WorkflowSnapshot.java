package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;

/** 工作流状态快照（查询用，不可变）。 */
public record WorkflowSnapshot(
        String workflowId,
        WorkflowState state,
        int runCount,
        int maxConcurrency,
        int peakConcurrency,
        int maxAttempts,
        long taskTimeoutMillis,
        String failureTask,
        String failureReason,
        long startedAt,
        long finishedAt,
        List<TaskSnapshot> tasks,
        boolean duplicateSubmission
) {
}
