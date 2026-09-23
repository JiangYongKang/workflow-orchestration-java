package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskState;

import java.util.concurrent.atomic.AtomicInteger;

/** 任务运行态（仅引擎内部使用）。 */
final class TaskRuntime {
    private final TaskDefinition definition;
    private volatile TaskState state = TaskState.PENDING;
    private final AtomicInteger attempts = new AtomicInteger(0);
    private volatile int finishOrder;
    private volatile long startMillis;
    private volatile long finishMillis;
    private volatile String lastMessage = "";

    TaskRuntime(TaskDefinition definition) {
        this.definition = definition;
    }

    TaskDefinition getDefinition() { return definition; }
    TaskState getState() { return state; }
    void setState(TaskState state) { this.state = state; }
    int getAttempts() { return attempts.get(); }
    int incrementAttempts() { return attempts.incrementAndGet(); }
    /** 恢复执行新一轮时重新计数（已完成任务不会走到这里）。 */
    void resetAttempts() { attempts.set(0); }
    int getFinishOrder() { return finishOrder; }
    void setFinishOrder(int finishOrder) { this.finishOrder = finishOrder; }
    long getStartMillis() { return startMillis; }
    void setStartMillis(long v) { this.startMillis = v; }
    long getFinishMillis() { return finishMillis; }
    void setFinishMillis(long v) { this.finishMillis = v; }
    String getLastMessage() { return lastMessage; }
    void setLastMessage(String v) { this.lastMessage = v; }
}
