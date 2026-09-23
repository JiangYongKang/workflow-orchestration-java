package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;

/** 工作流声明：工作流 id 与任务列表。 */
public record WorkflowSpec(String id, List<TaskSpec> tasks) {
}
