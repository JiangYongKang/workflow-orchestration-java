package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskContext;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskOutcome;

/** 受控的任务执行单元。 */
public interface TaskHandler {
    TaskOutcome execute(TaskContext context) throws InterruptedException;
}
