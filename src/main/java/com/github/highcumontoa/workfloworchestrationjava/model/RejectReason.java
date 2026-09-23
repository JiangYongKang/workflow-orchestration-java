package com.github.highcumontoa.workfloworchestrationjava.model;

/** 工作流登记被拒绝的原因（结论确定且可区分）。 */
public enum RejectReason {
    DUPLICATE_TASK,
    UNKNOWN_DEPENDENCY,
    CYCLIC_DEPENDENCY
}
