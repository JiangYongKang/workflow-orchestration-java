package com.github.highcumontoa.workfloworchestrationjava.service;

/** 查询/恢复一个未登记的工作流。 */
public class WorkflowNotFoundException extends RuntimeException {
    public WorkflowNotFoundException(String workflowId) {
        super("workflow not found: " + workflowId);
    }
}
