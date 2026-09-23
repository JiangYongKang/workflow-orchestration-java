package com.github.highcumontoa.workfloworchestrationjava.service;

/** 相同 id 但声明内容不同的重复提交时抛出。 */
public class WorkflowConflictException extends RuntimeException {
    public WorkflowConflictException(String workflowId) {
        super("workflow already exists with a different definition: " + workflowId);
    }
}
