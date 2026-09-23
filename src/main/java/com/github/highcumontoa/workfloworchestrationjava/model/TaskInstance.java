package com.github.highcumontoa.workfloworchestrationjava.model;

/**
 * 任务的运行态：状态、累计尝试次数、最近一次失败原因、所处运行轮次。
 *
 * <p>runEpoch 从 0 开始；每次失败恢复（FAILED -> PENDING）时加 1，
 * 用于受控任务区分“首次运行”与“恢复运行”，同时 totalAttempts 跨轮次累计不重置。
 */
public class TaskInstance {

    private final TaskSpec spec;
    private TaskState state = TaskState.PENDING;
    private int totalAttempts;
    private int runEpoch;
    private String lastError;

    public TaskInstance(TaskSpec spec) {
        this.spec = spec;
    }

    public TaskSpec getSpec() {
        return spec;
    }

    public TaskState getState() {
        return state;
    }

    public void setState(TaskState state) {
        this.state = state;
    }

    public int getTotalAttempts() {
        return totalAttempts;
    }

    public void setTotalAttempts(int totalAttempts) {
        this.totalAttempts = totalAttempts;
    }

    public int getRunEpoch() {
        return runEpoch;
    }

    public void setRunEpoch(int runEpoch) {
        this.runEpoch = runEpoch;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }
}
