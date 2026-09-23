package com.github.highcumontoa.workfloworchestrationjava.service;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskSpec;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存登记处：保存工作流声明与运行态。
 *
 * <p>提交幂等以工作流 id 为键：相同声明的重复提交返回同一份实例与执行记录；
 * 相同 id 但声明内容不同则拒绝（{@link WorkflowConflictException}），避免静默覆盖。
 * 并发提交下通过 putIfAbsent + 登记锁保证只产生一份执行记录。
 */
@Component
public class WorkflowRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRegistry.class);

    private final ConcurrentHashMap<String, WorkflowInstance> store = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> registerLocks = new ConcurrentHashMap<>();

    /** 幂等登记：相同声明返回已有实例；不同声明冲突报错。 */
    public WorkflowInstance register(WorkflowSpec spec) {
        Object lock = registerLocks.computeIfAbsent(spec.id(), k -> new Object());
        synchronized (lock) {
            WorkflowInstance existing = store.get(spec.id());
            if (existing != null) {
                if (sameDefinition(existing.getSpec(), spec)) {
                    log.info("[state-basis] idempotent submit: workflow '{}' already registered, "
                            + "returning the single existing instance (state={})",
                            spec.id(), existing.getState());
                    return existing;
                }
                log.warn("[trigger] workflow '{}' re-submitted with a different definition -> conflict",
                        spec.id());
                throw new WorkflowConflictException(spec.id());
            }
            WorkflowInstance created = new WorkflowInstance(spec);
            for (TaskSpec taskSpec : spec.tasks()) {
                created.getTasks().put(taskSpec.id(), new TaskInstance(taskSpec));
            }
            store.put(spec.id(), created);
            log.info("[state-basis] workflow '{}' registered with {} task(s), one execution record created",
                    spec.id(), spec.tasks().size());
            return created;
        }
    }

    public Optional<WorkflowInstance> find(String workflowId) {
        return Optional.ofNullable(store.get(workflowId));
    }

    private boolean sameDefinition(WorkflowSpec a, WorkflowSpec b) {
        if (a.tasks().size() != b.tasks().size()) {
            return false;
        }
        for (int i = 0; i < a.tasks().size(); i++) {
            TaskSpec x = a.tasks().get(i);
            TaskSpec y = b.tasks().get(i);
            if (!x.id().equals(y.id())
                    || !sameDeps(x.dependencies(), y.dependencies())
                    || x.action() != y.action()
                    || !intOf(x.failTimes()).equals(intOf(y.failTimes()))
                    || !longOf(x.sleepMillis()).equals(longOf(y.sleepMillis()))) {
                return false;
            }
        }
        return true;
    }

    private boolean sameDeps(List<String> a, List<String> b) {
        List<String> xa = a == null ? List.of() : a;
        List<String> xb = b == null ? List.of() : b;
        return xa.equals(xb);
    }

    private Integer intOf(Integer v) {
        return v == null ? 0 : v;
    }

    private Long longOf(Long v) {
        return v == null ? 0L : v;
    }
}
