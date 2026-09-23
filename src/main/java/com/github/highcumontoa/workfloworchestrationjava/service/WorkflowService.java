package com.github.highcumontoa.workfloworchestrationjava.service;

import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSpec;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 应用服务：校验、幂等提交、查询、失败恢复。
 *
 * <p>并发语义：同一工作流的执行由按 id 分桶的运行锁串行化——并发重复提交时只有一个线程驱动执行，
 * 其余线程等待同一终态，因此任何线程拿到的结论一致且只存在一份执行记录。
 */
@Service
public class WorkflowService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowService.class);

    private final WorkflowValidator validator;
    private final WorkflowRegistry registry;
    private final WorkflowEngine engine;
    private final ConcurrentHashMap<String, Object> runLocks = new ConcurrentHashMap<>();

    public WorkflowService(WorkflowValidator validator, WorkflowRegistry registry, WorkflowEngine engine) {
        this.validator = validator;
        this.registry = registry;
        this.engine = engine;
    }

    /**
     * 提交工作流：先校验（非法声明不会留下任何记录），再幂等登记，最后驱动到终态。
     * 相同声明的重复提交（含并发提交）返回同一份实例与同一个最终结论。
     */
    public WorkflowInstance submit(WorkflowSpec spec) {
        List<String> order = validator.validateAndOrder(spec);
        WorkflowInstance instance = registry.register(spec);
        return driveToTerminal(instance, order);
    }

    /** 查询工作流当前状态；不存在则抛出 {@link WorkflowNotFoundException}。 */
    public WorkflowInstance get(String workflowId) {
        return registry.find(workflowId)
                .orElseThrow(() -> {
                    log.warn("[trigger] query unknown workflow '{}'", workflowId);
                    return new WorkflowNotFoundException(workflowId);
                });
    }

    /**
     * 恢复失败的工作流：从已完成部分继续，已 SUCCEEDED 的任务不重复执行。
     * SUCCEEDED 的工作流恢复为幂等空操作；RUNNING 时等待其到达终态。
     */
    public WorkflowInstance resume(String workflowId) {
        WorkflowInstance instance = get(workflowId);
        List<String> order = validator.validateAndOrder(instance.getSpec());
        return driveToTerminal(instance, order);
    }

    private WorkflowInstance driveToTerminal(WorkflowInstance instance, List<String> order) {
        String id = instance.getSpec().id();
        Object runLock = runLocks.computeIfAbsent(id, k -> new Object());
        synchronized (runLock) {
            synchronized (instance) {
                if (instance.getState() == WorkflowState.RUNNING) {
                    log.info("[state-basis] workflow '{}' is being driven by another submit; waiting for its terminal state",
                            id);
                    waitTerminal(instance);
                    return instance;
                }
                if (instance.getState() == WorkflowState.SUCCEEDED) {
                    log.info("[state-basis] workflow '{}' already SUCCEEDED; submit is idempotent", id);
                    return instance;
                }
                // PENDING（首次）或 FAILED（恢复）都交给引擎；引擎内部负责断点续跑
                engine.run(instance, order);
                waitTerminal(instance);
                return instance;
            }
        }
    }

    private void waitTerminal(WorkflowInstance instance) {
        synchronized (instance) {
            while (instance.getState() == WorkflowState.RUNNING) {
                try {
                    instance.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for workflow "
                            + instance.getSpec().id(), e);
                }
            }
        }
    }
}
