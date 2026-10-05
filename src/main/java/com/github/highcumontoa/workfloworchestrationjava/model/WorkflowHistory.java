package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;

/** 工作流执行历史：按轮次追加，顺序稳定，记录不可变。 */
public record WorkflowHistory(
        String workflowId,
        List<WorkflowRunRecord> runs
) {
}
