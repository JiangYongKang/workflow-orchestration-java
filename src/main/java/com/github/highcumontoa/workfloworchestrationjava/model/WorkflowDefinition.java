package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;
import java.util.TreeSet;

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

    /**
     * 声明的规范化指纹：用于区分“原样重复提交”（幂等）与“同 id 不同声明”（冲突）。
     * 任务按名称排序、依赖集合排序，与任务书写顺序、依赖书写顺序无关；
     * 行为参数是有序列表，保持原序参与指纹。
     */
    public String fingerprint() {
        StringBuilder sb = new StringBuilder();
        sb.append("mc=").append(maxConcurrency)
                .append("|ma=").append(maxAttempts)
                .append("|to=").append(taskTimeoutMillis);
        if (tasks != null) {
            tasks.stream()
                    .sorted(java.util.Comparator.comparing(TaskDefinition::getName,
                            String.CASE_INSENSITIVE_ORDER))
                    .forEach(t -> sb.append("|task{")
                            .append(t.getName())
                            .append(",deps=").append(t.getDependsOn() == null
                                    ? "[]" : new TreeSet<>(t.getDependsOn()))
                            .append(",behavior=").append(t.getBehavior())
                            .append(",params=").append(t.getParams())
                            .append('}'));
        }
        return sb.toString();
    }
}
