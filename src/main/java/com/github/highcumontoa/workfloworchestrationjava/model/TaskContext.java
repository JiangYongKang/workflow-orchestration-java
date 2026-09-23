package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;

/** 单次任务执行尝试的上下文。 */
public final class TaskContext {
    private final String workflowId;
    private final String taskName;
    private final int attempt;
    private final String behavior;
    private final List<String> params;

    public TaskContext(String workflowId, String taskName, int attempt,
                       String behavior, List<String> params) {
        this.workflowId = workflowId;
        this.taskName = taskName;
        this.attempt = attempt;
        this.behavior = behavior;
        this.params = params;
    }

    public String getWorkflowId() { return workflowId; }
    public String getTaskName() { return taskName; }
    public int getAttempt() { return attempt; }
    public String getBehavior() { return behavior; }
    public List<String> getParams() { return params; }
}
