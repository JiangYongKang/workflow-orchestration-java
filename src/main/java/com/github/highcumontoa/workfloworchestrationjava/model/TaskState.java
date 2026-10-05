package com.github.highcumontoa.workfloworchestrationjava.model;

/** 任务状态。 */
public enum TaskState {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    SKIPPED,
    /** 因工作流被取消而未启动（终态，区别于失败连累的 SKIPPED）。 */
    CANCELLED
}
