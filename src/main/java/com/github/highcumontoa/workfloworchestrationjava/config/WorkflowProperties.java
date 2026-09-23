package com.github.highcumontoa.workfloworchestrationjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 可配置项：并发度、重试次数、单任务超时、重试间隔。 */
@ConfigurationProperties(prefix = "workflow")
public class WorkflowProperties {
    private int maxConcurrency = 4;
    private int maxAttempts = 3;
    private long taskTimeoutMillis = 5000;
    private long retryBackoffMillis = 100;
    private int engineThreads = 16;

    public int getMaxConcurrency() { return maxConcurrency; }
    public void setMaxConcurrency(int v) { this.maxConcurrency = v; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int v) { this.maxAttempts = v; }
    public long getTaskTimeoutMillis() { return taskTimeoutMillis; }
    public void setTaskTimeoutMillis(long v) { this.taskTimeoutMillis = v; }
    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(long v) { this.retryBackoffMillis = v; }
    public int getEngineThreads() { return engineThreads; }
    public void setEngineThreads(int v) { this.engineThreads = v; }
}
