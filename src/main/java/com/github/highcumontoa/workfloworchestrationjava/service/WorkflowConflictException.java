package com.github.highcumontoa.workfloworchestrationjava.service;

/** 同一 workflowId 提交了与既有记录不一致的任务声明。 */
public class WorkflowConflictException extends RuntimeException {
    public WorkflowConflictException(String workflowId) {
        super("workflow id '" + workflowId
                + "' already registered with a different definition");
    }
}
