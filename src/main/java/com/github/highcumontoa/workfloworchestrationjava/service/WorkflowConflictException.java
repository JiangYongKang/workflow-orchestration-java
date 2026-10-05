package com.github.highcumontoa.workfloworchestrationjava.service;

/**
 * 同一 workflowId 提交了与既有记录不一致的任务声明：
 * 不是幂等重复，必须显式报冲突（HTTP 409），不得返回对不上的旧结论。
 */
public class WorkflowConflictException extends RuntimeException {
    public WorkflowConflictException(String workflowId) {
        super("workflow '" + workflowId
                + "' already exists with a different definition; "
                + "identical resubmission is idempotent, divergent declaration is a conflict");
    }
}
