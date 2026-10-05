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

    /**
     * 提交并同步执行。
     *
     * <p>语义：
     * <ul>
     *   <li>非法声明（含非法并发/重试/超时取值）在登记前被拦截，不留下任何记录；</li>
     *   <li>同 id 且声明指纹一致：幂等返回既有结论，不产生第二次执行；</li>
     *   <li>同 id 但声明指纹不一致：抛 {@link WorkflowConflictException}（HTTP 409），
     *       绝不把对不上的旧结论当成新提交的结果返回。</li>
     * </ul>
     */
    public WorkflowSnapshot submit(WorkflowDefinition definition) {
        if (definition == null || definition.getWorkflowId() == null
                || definition.getWorkflowId().isBlank()) {
            throw new IllegalArgumentException("workflowId must not be blank");
        }
        String workflowId = definition.getWorkflowId();
        Object lock = idLocks.computeIfAbsent(workflowId, k -> new Object());
        synchronized (lock) {
            // 先校验（不登记）：非法声明无论是否重复提交都直接拒绝，不留半成品记录
            engine.validate(definition);
            WorkflowExecution existing = engine.get(workflowId);
            if (existing != null) {
                if (!existing.getDefinition().fingerprint().equals(definition.fingerprint())) {
                    log.warn("[{}] 提交冲突：同 id 但任务声明与既有记录不一致，"
                            + "拒绝并显式报冲突（不返回旧结论）", workflowId);
                    throw new WorkflowConflictException(workflowId);
                }
                log.info("[{}] 重复提交被幂等拦截：声明指纹一致，返回既有执行记录，"
                        + "不产生第二次执行（状态={}）", workflowId, existing.getState());
                return engine.snapshot(existing, true).orElseThrow();
            }
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
     * 取消工作流（幂等）：正在执行的安全收敛、未启动的不再启动、已完成的保留；
     * CANCELLED 是稳定终态，之后的恢复请求不会把它重新拉起。
     * 重复取消、对已成功工作流的取消均为幂等空操作，返回当前快照。
     */
    public WorkflowSnapshot cancel(String workflowId) {
        WorkflowExecution execution = engine.get(workflowId);
        if (execution == null) {
            log.info("[{}] 取消失败：工作流未登记", workflowId);
            throw new WorkflowNotFoundException(workflowId);
        }
        engine.cancelAndAwait(execution);
        WorkflowSnapshot s = engine.snapshot(execution, false).orElseThrow();
        log.info("[{}] 取消请求处理完毕：状态={}，runCount={}", workflowId, s.state(), s.runCount());
        return s;
    }

    /**
     * 失败恢复：仅 FAILED 工作流会再执行一轮，从已完成任务继续；
     * 非 FAILED（含执行中、成功、已取消）状态下幂等返回当前快照。
     * CANCELLED 为稳定终态，恢复请求不会把它重新拉起。
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
