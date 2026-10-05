package com.github.highcumontoa.workfloworchestrationjava.engine;

import com.github.highcumontoa.workfloworchestrationjava.config.WorkflowProperties;
import com.github.highcumontoa.workfloworchestrationjava.model.TaskDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * 工作流声明的规范化指纹：用于区分“原样重复提交”与“同 id 不同声明的冲突提交”。
 *
 * <p>规范化规则（语义等价的声明得到相同指纹）：
 * <ul>
 *   <li>执行参数（并发/重试/超时）解析为生效值（null 回落全局默认）后参与指纹；</li>
 *   <li>任务按名称排序，依赖集合排序，行为参数保持声明顺序（位置语义）；</li>
 *   <li>behavior 为 null/空白时按缺省（恒成功）归一。</li>
 * </ul>
 */
public final class DefinitionFingerprint {

    private DefinitionFingerprint() {
    }

    public static String of(WorkflowDefinition d, WorkflowProperties props) {
        int concurrency = d.getMaxConcurrency() != null
                ? d.getMaxConcurrency() : props.getMaxConcurrency();
        int attempts = d.getMaxAttempts() != null
                ? d.getMaxAttempts() : props.getMaxAttempts();
        long timeout = d.getTaskTimeoutMillis() != null
                ? d.getTaskTimeoutMillis() : props.getTaskTimeoutMillis();

        List<String> taskParts = new ArrayList<>();
        for (TaskDefinition t : d.getTasks()) {
            String deps = t.getDependsOn() == null
                    ? "" : String.join(",", new TreeSet<>(t.getDependsOn()));
            String behavior = (t.getBehavior() == null || t.getBehavior().isBlank())
                    ? "" : t.getBehavior();
            String params = t.getParams() == null ? "" : String.join(",", t.getParams());
            taskParts.add(t.getName() + "|" + deps + "|" + behavior + "|" + params);
        }
        taskParts.sort(String::compareTo);
        return "c=" + concurrency + ";a=" + attempts + ";t=" + timeout
                + ";tasks=" + String.join(";", taskParts);
    }
}
