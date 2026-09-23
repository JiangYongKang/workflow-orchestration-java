package com.github.highcumontoa.workfloworchestrationjava.validation;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskSpec;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 工作流声明校验：
 * <ol>
 *   <li>请求结构合法（工作流 id 非空、任务列表非空、任务 id 非空）；</li>
 *   <li>不存在同名任务重复声明（{@link RejectReason#DUPLICATE_TASK}）；</li>
 *   <li>每个依赖都指向已声明的任务（{@link RejectReason#UNKNOWN_DEPENDENCY}）；</li>
 *   <li>依赖图无环（{@link RejectReason#CYCLIC_DEPENDENCY}）。</li>
 * </ol>
 * 校验通过后用 Kahn 算法给出满足全部依赖关系的拓扑执行顺序。
 */
@Component
public class WorkflowValidator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowValidator.class);

    /**
     * 校验并返回满足依赖关系的拓扑执行顺序。
     *
     * @throws WorkflowValidationException 校验失败，原因见 {@link RejectReason}
     */
    public List<String> validateAndOrder(WorkflowSpec spec) {
        if (spec == null || spec.id() == null || spec.id().isBlank()) {
            throw reject(RejectReason.INVALID_REQUEST, "workflow id must not be blank");
        }
        if (spec.tasks() == null || spec.tasks().isEmpty()) {
            throw reject(RejectReason.INVALID_REQUEST,
                    "workflow '" + spec.id() + "' must declare at least one task");
        }

        // 1) 任务 id 非空 + 同名任务重复声明
        Map<String, TaskSpec> byId = new HashMap<>();
        for (TaskSpec task : spec.tasks()) {
            if (task == null || task.id() == null || task.id().isBlank()) {
                throw reject(RejectReason.INVALID_REQUEST,
                        "workflow '" + spec.id() + "' contains a task with blank id");
            }
            if (byId.put(task.id(), task) != null) {
                throw reject(RejectReason.DUPLICATE_TASK,
                        "duplicate task declaration '" + task.id()
                                + "' in workflow '" + spec.id() + "'");
            }
        }

        // 2) 未知依赖
        for (TaskSpec task : spec.tasks()) {
            List<String> deps = task.dependencies() == null ? List.of() : task.dependencies();
            for (String dep : deps) {
                if (dep == null || dep.isBlank()) {
                    throw reject(RejectReason.INVALID_REQUEST,
                            "task '" + task.id() + "' declares a blank dependency");
                }
                if (!byId.containsKey(dep)) {
                    throw reject(RejectReason.UNKNOWN_DEPENDENCY,
                            "task '" + task.id() + "' depends on unknown task '" + dep
                                    + "' in workflow '" + spec.id() + "'");
                }
            }
        }

        // 3) Kahn 拓扑排序：入度为 0 即所有依赖已满足
        Map<String, Set<String>> dependents = new HashMap<>();
        Map<String, Integer> indegree = new HashMap<>();
        for (String id : byId.keySet()) {
            dependents.put(id, new LinkedHashSet<>());
            indegree.put(id, 0);
        }
        for (TaskSpec task : spec.tasks()) {
            Set<String> uniqDeps = new LinkedHashSet<>(
                    task.dependencies() == null ? List.of() : task.dependencies());
            indegree.put(task.id(), uniqDeps.size());
            for (String dep : uniqDeps) {
                dependents.get(dep).add(task.id());
            }
        }

        // 保持声明顺序作为同层先后，结论稳定可复现
        Deque<String> ready = new ArrayDeque<>();
        for (TaskSpec task : spec.tasks()) {
            if (indegree.get(task.id()) == 0) {
                ready.add(task.id());
            }
        }
        List<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            String id = ready.poll();
            order.add(id);
            for (String downstream : dependents.get(id)) {
                int left = indegree.merge(downstream, -1, Integer::sum);
                if (left == 0) {
                    ready.add(downstream);
                }
            }
        }

        if (order.size() != byId.size()) {
            Set<String> unresolved = new HashSet<>(byId.keySet());
            unresolved.removeAll(order);
            throw reject(RejectReason.CYCLIC_DEPENDENCY,
                    "cyclic dependency detected in workflow '" + spec.id()
                            + "', tasks never become ready: " + unresolved);
        }

        log.info("workflow '{}' validated, execution order: {}", spec.id(), order);
        return List.copyOf(order);
    }

    private WorkflowValidationException reject(RejectReason reason, String message) {
        log.warn("reject workflow registration: reason={}, detail={}", reason, message);
        return new WorkflowValidationException(reason, message);
    }
}
