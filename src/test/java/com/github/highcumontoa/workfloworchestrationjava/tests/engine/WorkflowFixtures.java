package com.github.highcumontoa.workfloworchestrationjava.tests.engine;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/** 测试构造辅助。 */
final class WorkflowFixtures {

    private WorkflowFixtures() {
    }

    static TaskDefinition task(String name, String behavior, List<String> params,
                               String... deps) {
        return new TaskDefinition(name, new HashSet<>(Arrays.asList(deps)), behavior, params);
    }

    static TaskDefinition task(String name, String... deps) {
        return task(name, "ALWAYS_SUCCEED", List.of(), deps);
    }

    static WorkflowDefinition workflow(String id, List<TaskDefinition> tasks,
                                       Integer concurrency, Integer attempts,
                                       Long timeout) {
        return new WorkflowDefinition(id, new ArrayList<>(tasks), concurrency, attempts, timeout);
    }
}
