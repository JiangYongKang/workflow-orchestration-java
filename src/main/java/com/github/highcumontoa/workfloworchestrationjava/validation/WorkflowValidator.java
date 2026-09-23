package com.github.highcumontoa.workfloworchestrationjava.validation;

import com.github.highcumontoa.workfloworchestrationjava.model.RejectReason;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 工作流登记校验。拒绝原因按固定顺序判定，保证结论确定、可复现：
 * <ol>
 *   <li>{@link RejectReason#DUPLICATE_TASK} 同名任务重复声明</li>
 *   <li>{@link RejectReason#UNKNOWN_DEPENDENCY} 依赖了未声明的任务</li>
 *   <li>{@link RejectReason#CYCLIC_DEPENDENCY} 依赖图存在环</li>
 * </ol>
 */
@Component
public class WorkflowValidator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowValidator.class);

    public void validate(WorkflowDefinition definition) {
        if (definition == null || definition.getWorkflowId() == null
                || definition.getWorkflowId().isBlank()) {
            throw new IllegalArgumentException("workflowId must not be blank");
        }
        List<TaskDefinition> tasks = definition.getTasks();
        if (tasks == null || tasks.isEmpty()) {
            throw new IllegalArgumentException("tasks must not be empty for workflow "
                    + definition.getWorkflowId());
        }

        // 1) 同名任务重复声明
        Set<String> seen = new HashSet<>();
        for (TaskDefinition task : tasks) {
            if (task.getName() == null || task.getName().isBlank()) {
                throw new IllegalArgumentException("task name must not be blank in workflow "
                        + definition.getWorkflowId());
            }
            if (!seen.add(task.getName())) {
                log.warn("[{}] 登记被拒绝: 同名任务重复声明 name={}",
                        definition.getWorkflowId(), task.getName());
                throw new ValidationException(RejectReason.DUPLICATE_TASK,
                        "duplicate task declaration: " + task.getName());
            }
        }

        // 2) 未知依赖
        for (TaskDefinition task : tasks) {
            Set<String> deps = task.getDependsOn();
            if (deps == null) {
                continue;
            }
            for (String dep : deps) {
                if (!seen.contains(dep)) {
                    log.warn("[{}] 登记被拒绝: 任务 {} 引用了不存在的任务 {}",
                            definition.getWorkflowId(), task.getName(), dep);
                    throw new ValidationException(RejectReason.UNKNOWN_DEPENDENCY,
                            "task '" + task.getName() + "' depends on unknown task: " + dep);
                }
                if (dep.equals(task.getName())) {
                    log.warn("[{}] 登记被拒绝: 任务 {} 依赖自身，构成循环",
                            definition.getWorkflowId(), task.getName());
                    throw new ValidationException(RejectReason.CYCLIC_DEPENDENCY,
                            "task '" + task.getName() + "' depends on itself");
                }
            }
        }

        // 3) 循环依赖：DFS 三色标记，收集环上的节点便于解释
        Map<String, TaskDefinition> byName = new HashMap<>();
        for (TaskDefinition task : tasks) {
            byName.put(task.getName(), task);
        }
        Map<String, Integer> color = new HashMap<>(); // 0=白 1=灰(在当前路径) 2=黑(已完成)
        for (TaskDefinition task : tasks) {
            color.put(task.getName(), 0);
        }
        for (TaskDefinition task : tasks) {
            if (color.get(task.getName()) == 0) {
                dfsCycle(definition.getWorkflowId(), task.getName(), byName, color);
            }
        }
        log.info("[{}] 登记校验通过，任务数={}", definition.getWorkflowId(), tasks.size());
    }

    private void dfsCycle(String workflowId, String node,
                          Map<String, TaskDefinition> byName, Map<String, Integer> color) {
        color.put(node, 1);
        Set<String> deps = byName.get(node).getDependsOn();
        if (deps != null) {
            for (String dep : deps) {
                int depColor = color.getOrDefault(dep, 2);
                if (depColor == 1) {
                    log.warn("[{}] 登记被拒绝: 检测到循环依赖，环中包含 {} -> {}",
                            workflowId, node, dep);
                    throw new ValidationException(RejectReason.CYCLIC_DEPENDENCY,
                            "cyclic dependency detected at edge: " + node + " -> " + dep);
                }
                if (depColor == 0) {
                    dfsCycle(workflowId, dep, byName, color);
                }
            }
        }
        color.put(node, 2);
    }
}
