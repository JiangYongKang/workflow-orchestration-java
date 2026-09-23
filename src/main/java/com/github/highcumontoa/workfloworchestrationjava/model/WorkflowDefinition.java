package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;

/** 工作流声明：id、任务列表、执行参数。 */
public final class WorkflowDefinition {
    private final String workflowId;
    private final List<TaskDefinition> tasks;
    private final Integer maxConcurrency;
    private final Integer maxAttempts;
    private final Long taskTimeoutMillis;

    public WorkflowDefinition(String workflowId, List<TaskDefinition> tasks,
                              Integer maxConcurrency, Integer maxAttempts, Long taskTimeoutMillis) {
        this.workflowId = workflowId;
        this.tasks = tasks;
        this.maxConcurrency = maxConcurrency;
        this.maxAttempts = maxAttempts;
        this.taskTimeoutMillis = taskTimeoutMillis;
    }

    public String getWorkflowId() { return workflowId; }
    public List<TaskDefinition> getTasks() { return tasks; }
    public Integer getMaxConcurrency() { return maxConcurrency; }
    public Integer getMaxAttempts() { return maxAttempts; }
    public Long getTaskTimeoutMillis() { return taskTimeoutMillis; }
}
