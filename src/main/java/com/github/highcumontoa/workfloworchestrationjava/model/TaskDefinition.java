package com.github.highcumontoa.workfloworchestrationjava.model;

import java.util.List;
import java.util.Set;

/** 任务声明：名称、依赖、行为类型、行为参数。 */
public final class TaskDefinition {
    private final String name;
    private final Set<String> dependsOn;
    private final String behavior;
    private final List<String> params;

    public TaskDefinition(String name, Set<String> dependsOn, String behavior, List<String> params) {
        this.name = name;
        this.dependsOn = dependsOn;
        this.behavior = behavior;
        this.params = params;
    }

    public String getName() { return name; }
    public Set<String> getDependsOn() { return dependsOn; }
    public String getBehavior() { return behavior; }
    public List<String> getParams() { return params; }
}
