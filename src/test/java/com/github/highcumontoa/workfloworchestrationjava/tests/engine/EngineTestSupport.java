package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.config.WorkflowProperties;
import com.github.highcumontoa.workfloworchestrationjava.engine.DeterministicTaskHandler;
import com.github.highcumontoa.workfloworchestrationjava.engine.TaskExecutor;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidator;

/** 测试中手工装配引擎，默认退避为 0 保证快速、确定。 */
final class EngineTestSupport {

    private EngineTestSupport() {
    }

    static WorkflowEngine newEngine() {
        WorkflowProperties props = new WorkflowProperties();
        props.setRetryBackoffMillis(0);
        props.setTaskTimeoutMillis(500);
        props.setMaxAttempts(3);
        props.setMaxConcurrency(2);
        TaskExecutor executor = new TaskExecutor(new DeterministicTaskHandler(), props);
        return new WorkflowEngine(new WorkflowValidator(), executor, props);
    }
}
