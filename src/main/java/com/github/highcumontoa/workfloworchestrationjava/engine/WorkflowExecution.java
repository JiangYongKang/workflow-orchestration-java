package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowRunRecord;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** 工作流执行记录（仅引擎内部使用）。 */
public final class WorkflowExecution {
    private final WorkflowDefinition definition;
    private final String fingerprint;
    private volatile WorkflowState state = WorkflowState.REGISTERED;
    private final AtomicInteger runCount = new AtomicInteger(0);
    private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
    /** 追加式执行历史：每轮结束追加一条，记录不可变，顺序即轮次顺序。 */
    private final List<WorkflowRunRecord> history = new CopyOnWriteArrayList<>();
    private volatile String failureTask;
    private volatile String failureReason = "";
    private volatile long startedAt;
    private volatile long finishedAt;
    private volatile int peakConcurrency;
    /** 首次执行前为 null；执行后保留全部任务运行态，供查询与恢复使用。 */
    private Map<String, TaskRuntime> taskRuntimes;

    WorkflowExecution(WorkflowDefinition definition, String fingerprint) {
        this.definition = definition;
        this.fingerprint = fingerprint;
    }

    public WorkflowDefinition getDefinition() { return definition; }
    public String getFingerprint() { return fingerprint; }
    public WorkflowState getState() { return state; }
    public void setState(WorkflowState state) { this.state = state; }
    public int incrementRunCount() { return runCount.incrementAndGet(); }
    public int getRunCount() { return runCount.get(); }
    public boolean requestCancel() { return cancelRequested.compareAndSet(false, true); }
    public boolean isCancelRequested() { return cancelRequested.get(); }
    void appendRunRecord(WorkflowRunRecord record) { history.add(record); }
    List<WorkflowRunRecord> runRecords() { return List.copyOf(history); }
    public String getFailureTask() { return failureTask; }
    public void setFailureTask(String v) { this.failureTask = v; }
    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String v) { this.failureReason = v; }
    public long getStartedAt() { return startedAt; }
    public void setStartedAt(long v) { this.startedAt = v; }
    public long getFinishedAt() { return finishedAt; }
    public void setFinishedAt(long v) { this.finishedAt = v; }
    public int getPeakConcurrency() { return peakConcurrency; }
    public void setPeakConcurrency(int v) { this.peakConcurrency = v; }

    /**
     * 提供本次执行要使用的运行态：
     * <ul>
     *   <li>首次执行：挂载并返回新建的 {@code fresh}；</li>
     *   <li>恢复执行：忽略 {@code fresh}，继续使用上一轮的运行态——
     *       SUCCEEDED 任务保留其尝试计数与结果，FAILED/SKIPPED 由引擎原地重置。</li>
     * </ul>
     */
    synchronized Map<String, TaskRuntime> attachTaskRuntimes(Map<String, TaskRuntime> fresh) {
        if (taskRuntimes == null) {
            taskRuntimes = fresh;
            return fresh;
        }
        return taskRuntimes;
    }

    synchronized Map<String, TaskRuntime> currentTaskRuntimes() {
        return taskRuntimes;
    }
}
