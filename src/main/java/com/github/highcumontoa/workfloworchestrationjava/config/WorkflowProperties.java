package com.github.highcumontoa.workfloworchestrationjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 可配置项：workflow.orchestration.* */
@ConfigurationProperties(prefix = "workflow.orchestration")
public class WorkflowProperties {

    private int maxConcurrency = 4;
    private int maxAttempts = 3;
    private long retryBackoffMillis = 50;
    private long taskTimeoutMillis = 5_000;

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getRetryBackoffMillis() {
        return retryBackoffMillis;
    }

    public void setRetryBackoffMillis(long retryBackoffMillis) {
        this.retryBackoffMillis = retryBackoffMillis;
    }

    public long getTaskTimeoutMillis() {
        return taskTimeoutMillis;
    }

    public void setTaskTimeoutMillis(long taskTimeoutMillis) {
        this.taskTimeoutMillis = taskTimeoutMillis;
    }
}
