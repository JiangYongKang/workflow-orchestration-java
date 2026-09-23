package com.github.highcumontoa.workfloworchestrationjava.service;

import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowExecution;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 对外门面：提交（幂等）、查询、恢复。
 *
 * <p>幂等语义（在并发与多次查询下结论稳定）：
 * <ul>
 *   <li>相同 workflowId 的重复提交不会产生第二份执行记录：抢到锁的第一个请求完成
 *       登记与执行；其他并发请求在同一把 id 锁上等待，随后拿到标记
 *       {@code duplicateSubmission=true} 的同一份执行结论；</li>
 *   <li>每个 workflowId 持有独立锁，不同工作流之间不互相阻塞；</li>
 *   <li>非法定义在校验阶段即被拒绝，不会留下任何记录，可再次提交。</li>
 * </ul>
 */
@Service
public class WorkflowService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowService.class);

    private final WorkflowEngine engine;
    private final ConcurrentHashMap<String, Object> idLocks = new ConcurrentHashMap<>();

    public WorkflowService(WorkflowEngine engine) {
        this.engine = engine;
    }

    /** 提交并同步执行；重复/并发提交只产生一份执行记录。 */
    public WorkflowSnapshot submit(WorkflowDefinition definition) {
        if (definition == null || definition.getWorkflowId() == null
                || definition.getWorkflowId().isBlank()) {
            throw new IllegalArgumentException("workflowId must not be blank");
        }
        String workflowId = definition.getWorkflowId();
        Object lock = idLocks.computeIfAbsent(workflowId, k -> new Object());
        synchronized (lock) {
            WorkflowExecution existing = engine.get(workflowId);
            if (existing != null) {
                log.info("[{}] 重复提交被幂等拦截：返回既有执行记录，不产生第二次执行（状态={}）",
                        workflowId, existing.getState());
                return engine.snapshot(existing, true).orElseThrow();
            }
            // register 内先做校验：非法定义抛 ValidationException，不留下任何记录
            WorkflowExecution execution = engine.register(definition);
            engine.execute(execution);
            return engine.snapshot(execution, false).orElseThrow();
        }
    }

    /** 查询；不存在抛 {@link WorkflowNotFoundException}。查询不加锁，可在执行中进行。 */
    public WorkflowSnapshot query(String workflowId) {
        Optional<WorkflowSnapshot> snapshot = engine.snapshot(workflowId);
        if (snapshot.isEmpty()) {
            log.info("[{}] 查询失败：工作流未登记", workflowId);
            throw new WorkflowNotFoundException(workflowId);
        }
        WorkflowSnapshot s = snapshot.get();
        log.info("[{}] 查询结论：状态={}，runCount={}，失败任务={}，峰值并发={}",
                workflowId, s.state(), s.runCount(), s.failureTask(), s.peakConcurrency());
        return s;
    }

    /**
     * 失败恢复：仅 FAILED 工作流会再执行一轮，从已完成任务继续；
     * 非 FAILED（含执行中、成功）状态下幂等返回当前快照。
     */
    public WorkflowSnapshot resume(String workflowId) {
        WorkflowExecution execution = engine.get(workflowId);
        if (execution == null) {
            throw new WorkflowNotFoundException(workflowId);
        }
        Object lock = idLocks.computeIfAbsent(workflowId, k -> new Object());
        synchronized (lock) {
            if (execution.getState() != WorkflowState.FAILED) {
                log.info("[{}] 恢复请求被幂等忽略：当前状态={}，仅 FAILED 可恢复",
                        workflowId, execution.getState());
                return engine.snapshot(execution, false).orElseThrow();
            }
            log.info("[{}] 接受恢复请求：上轮失败任务={}，原因={}",
                    workflowId, execution.getFailureTask(), execution.getFailureReason());
            engine.execute(execution);
            return engine.snapshot(execution, false).orElseThrow();
        }
    }
}
