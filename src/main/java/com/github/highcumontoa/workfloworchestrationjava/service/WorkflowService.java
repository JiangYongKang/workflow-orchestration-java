package com.github.highcumontoa.workfloworchestrationjava.service;

import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowEngine;
import com.github.highcumontoa.workfloworchestrationjava.engine.WorkflowExecution;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowHistory;
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
            // 无论 id 新旧，非法定义一律先挡掉：不会留下任何记录，可修正后再次提交
            engine.validate(definition);
            WorkflowExecution existing = engine.get(workflowId);
            if (existing != null) {
                String incoming = engine.fingerprintOf(definition);
                if (!existing.getFingerprint().equals(incoming)) {
                    log.warn("[{}] 提交冲突：同一 id 提交了与既有记录不一致的任务声明，"
                                    + "按 409 冲突拒绝（既有指纹={}，新指纹={}）",
                            workflowId, existing.getFingerprint(), incoming);
                    throw new WorkflowConflictException(workflowId);
                }
                log.info("[{}] 重复提交被幂等拦截：声明与既有记录一致，返回既有执行结论，"
                                + "不产生第二次执行（状态={}）",
                        workflowId, existing.getState());
                return engine.snapshot(existing, true).orElseThrow();
            }
            // register 内会再次校验（防御性）；非法定义抛异常，不留下任何记录
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
     * 取消一个工作流（幂等，CANCELLED 为稳定终态）：
     * <ul>
     *   <li>执行中：先在锁外置取消标记——在途任务安全收敛（单尝试仍受超时硬上限约束）、
     *       未启动任务不再开始、已完成任务结果保留；随后在同一把 id 锁上等待本轮收敛完成；</li>
     *   <li>FAILED：直接流转为 CANCELLED，之后的恢复请求不能再把它拉起来；</li>
     *   <li>SUCCEEDED：取消到达太晚，幂等返回既有成功结论；</li>
     *   <li>CANCELLED：重复取消幂等返回，不产生额外影响。</li>
     * </ul>
     */
    public WorkflowSnapshot cancel(String workflowId) {
        WorkflowExecution execution = engine.get(workflowId);
        if (execution == null) {
            log.info("[{}] 取消失败：工作流未登记", workflowId);
            throw new WorkflowNotFoundException(workflowId);
        }
        Object lock = idLocks.computeIfAbsent(workflowId, k -> new Object());
        // 锁外先置标记：执行中的调度循环无需等锁即可看到取消请求并开始收敛
        boolean firstCancel = execution.requestCancel();
        synchronized (lock) {
            // 若本轮执行正在进行，此处等待其收敛；拿到锁时状态必为终态
            WorkflowState state = execution.getState();
            switch (state) {
                case CANCELLED -> log.info("[{}] 取消请求幂等忽略：工作流已处于 CANCELLED 终态",
                        workflowId);
                case SUCCEEDED -> log.info("[{}] 取消请求到达时工作流已 SUCCEEDED："
                        + "成功终态不可取消，幂等返回既有结论", workflowId);
                case FAILED -> {
                    execution.setState(WorkflowState.CANCELLED);
                    log.info("[{}] 工作流状态流转 FAILED -> CANCELLED，依据=收到取消请求："
                            + "失败终态被取消覆盖，后续恢复请求不再生效", workflowId);
                }
                default -> {
                    // 防御性兜底：执行与取消共用同一把 id 锁，理论上不会到达这里
                    execution.setState(WorkflowState.CANCELLED);
                    log.warn("[{}] 工作流状态流转 {} -> CANCELLED（防御性兜底，依据=取消请求）",
                            workflowId, state);
                }
            }
            if (firstCancel) {
                log.info("[{}] 取消请求已受理并收敛完成，当前状态={}", workflowId,
                        execution.getState());
            }
            return engine.snapshot(execution, false).orElseThrow();
        }
    }

    /** 查询追加式执行历史；不存在抛 {@link WorkflowNotFoundException}。 */
    public WorkflowHistory history(String workflowId) {
        return engine.history(workflowId)
                .orElseThrow(() -> new WorkflowNotFoundException(workflowId));
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
