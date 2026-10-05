package com.github.highcumontoa.workfloworchestrationjava.model;

/** 单个执行轮中某任务的记录（不可变，历史查询用）。 */
public record TaskRunRecord(
        String name,
        TaskState state,
        int attempts,
        long startedAtEpochMillis,
        long finishedAtEpochMillis,
        long durationMillis,
        String message
) {
}
