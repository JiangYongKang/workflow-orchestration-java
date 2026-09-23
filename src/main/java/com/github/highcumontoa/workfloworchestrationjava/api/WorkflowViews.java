package com.github.highcumontoa.workfloworchestrationjava.api;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskAction;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;

import java.util.List;

/** REST 视图对象集合：请求体与响应体均为不可变 record，字段即结论与依据。 */
public final class WorkflowViews {

    private WorkflowViews() {
    }

    public record TaskRequest(String id,
                              List<String> dependencies,
                              TaskAction action,
                              Integer failTimes,
                              Long sleepMillis) {
    }

    public record SubmitRequest(String id, List<TaskRequest> tasks) {
    }

    public record TaskView(String id,
                           TaskState state,
                           int totalAttempts,
                           int runEpoch,
                           String lastError) {

        static TaskView from(TaskInstance t) {
            return new TaskView(t.getSpec().id(), t.getState(), t.getTotalAttempts(),
                    t.getRunEpoch(), t.getLastError());
        }
    }

    public record WorkflowView(String id,
                               WorkflowState state,
                               String failureReason,
                               List<TaskView> tasks) {

        public static WorkflowView from(WorkflowInstance i) {
            List<TaskView> tasks = i.taskList().stream().map(TaskView::from).toList();
            return new WorkflowView(i.getSpec().id(), i.getState(), i.getFailureReason(), tasks);
        }
    }

    /** 错误响应：reason 为机器可区分的拒绝原因，message 为可读依据。 */
    public record ErrorView(String reason, String message) {
    }
}
