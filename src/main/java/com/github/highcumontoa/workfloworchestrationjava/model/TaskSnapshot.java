package com.github.highcumontoa.workfloworchestrationjava.model;

/** 任务状态快照（查询用，不可变）。 */
public record TaskSnapshot(
        String name,
        TaskState state,
        int attempts,
        long startedAtEpochMillis,
        long finishedAtEpochMillis,
        long durationMillis,
        String message
) {
}
