package com.github.highcumontoa.workfloworchestrationjava.model;

/** 工作流状态。 */
public enum WorkflowState {
    REGISTERED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    /** 取消后的稳定终态：不可恢复、不可再执行。 */
    CANCELLED
}
