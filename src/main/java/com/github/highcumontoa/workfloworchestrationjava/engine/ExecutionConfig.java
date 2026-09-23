package com.github.highcumontoa.workfloworchestrationjava.engine;

/**
 * 执行器全局配置。
 *
 * @param maxConcurrency 并发执行任务数的硬上限
 * @param maxAttempts    单任务最大尝试次数（首次执行 + 重试），上限到点必须稳定失败
 * @param retryBackoffMillis 临时错误后下一次尝试前的固定等待
 * @param taskTimeoutMillis  单任务单次尝试的耗时上限，超时按一次失败尝试计
 */
public record ExecutionConfig(int maxConcurrency,
                              int maxAttempts,
                              long retryBackoffMillis,
                              long taskTimeoutMillis) {
}
