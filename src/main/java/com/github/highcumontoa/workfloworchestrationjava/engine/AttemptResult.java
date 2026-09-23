package com.github.highcumontoa.workfloworchestrationjava.engine;

/**
 * 任务单次尝试的结果。
 *
 * @param success      本次尝试是否成功
 * @param retryable    失败是否可重试（临时错误/超时为 true，永久错误为 false）
 * @param errorMessage 失败原因描述
 */
public record AttemptResult(boolean success, boolean retryable, String errorMessage) {

    public static AttemptResult ok() {
        return new AttemptResult(true, false, null);
    }

    public static AttemptResult failure(boolean retryable, String errorMessage) {
        return new AttemptResult(false, retryable, errorMessage);
    }
}
