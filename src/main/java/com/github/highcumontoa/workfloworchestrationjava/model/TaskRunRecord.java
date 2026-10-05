package com.github.highcumontoa.workfloworchestrationjava.model;

/** 单个执行轮中某任务的留痕（不可变）：尝试次数、耗时与结论。 */
public record TaskRunRecord(
        String name,
        TaskState finalState,
        int attempts,
        long startedAtEpochMillis,
        long finishedAtEpochMillis,
        long durationMillis,
        String message
) {
}
