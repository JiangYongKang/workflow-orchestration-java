package com.github.highcumontoa.workfloworchestrationjava.validation;

/** 工作流声明被拒绝的可区分原因。 */
public enum RejectReason {
    /** 同名任务重复声明。 */
    DUPLICATE_TASK,
    /** 依赖引用了不存在的任务。 */
    UNKNOWN_DEPENDENCY,
    /** 任务依赖构成环。 */
    CYCLIC_DEPENDENCY,
    /** 请求结构非法（空 id、空任务列表等）。 */
    INVALID_REQUEST
}
