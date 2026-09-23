package com.github.highcumontoa.workfloworchestrationjava.model;

/** 一次任务尝试的结果：成功或带原因的失败。 */
public final class TaskOutcome {
    private final boolean success;
    private final String message;

    private TaskOutcome(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public static TaskOutcome success(String message) {
        return new TaskOutcome(true, message);
    }

    public static TaskOutcome failure(String message) {
        return new TaskOutcome(false, message);
    }

    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
}
