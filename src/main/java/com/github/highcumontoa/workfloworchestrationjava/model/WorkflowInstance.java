package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 工作流的运行态：整体状态、各任务运行态、失败原因。 */
public class WorkflowInstance {

    private final WorkflowSpec spec;
    private final Map<String, TaskInstance> tasks = new LinkedHashMap<>();
    private WorkflowState state = WorkflowState.PENDING;
    private String failureReason;

    public WorkflowInstance(WorkflowSpec spec) {
        this.spec = spec;
    }

    public WorkflowSpec getSpec() {
        return spec;
    }

    public Map<String, TaskInstance> getTasks() {
        return tasks;
    }

    public WorkflowState getState() {
        return state;
    }

    public void setState(WorkflowState state) {
        this.state = state;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public List<TaskInstance> taskList() {
        return List.copyOf(tasks.values());
    }
}
