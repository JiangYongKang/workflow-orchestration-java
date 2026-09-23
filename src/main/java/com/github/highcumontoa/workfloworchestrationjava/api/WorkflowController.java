package com.github.highcumontoa.workfloworchestrationjava.api;

import com.github.highcumontoa.workfloworchestrationjava.model.TaskSpec;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowInstance;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSpec;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 工作流 HTTP 入口：
 * <ul>
 *   <li>POST /api/workflows：校验 + 幂等提交，并同步返回终态结论；</li>
 *   <li>GET  /api/workflows/{id}：查询当前状态（重复查询结论一致）；</li>
 *   <li>POST /api/workflows/{id}/resume：失败恢复，从已完成部分继续。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowService service;

    public WorkflowController(WorkflowService service) {
        this.service = service;
    }

    @PostMapping
    public WorkflowViews.WorkflowView submit(@RequestBody WorkflowViews.SubmitRequest request) {
        List<TaskSpec> tasks = request.tasks() == null ? List.of()
                : request.tasks().stream()
                .map(t -> new TaskSpec(t.id(), t.dependencies() == null ? List.of() : t.dependencies(),
                        t.action(), t.failTimes(), t.sleepMillis()))
                .toList();
        WorkflowSpec spec = new WorkflowSpec(request.id(), tasks);
        WorkflowInstance instance = service.submit(spec);
        return WorkflowViews.WorkflowView.from(instance);
    }

    @GetMapping("/{id}")
    public WorkflowViews.WorkflowView get(@PathVariable String id) {
        return WorkflowViews.WorkflowView.from(service.get(id));
    }

    @PostMapping("/{id}/resume")
    public WorkflowViews.WorkflowView resume(@PathVariable String id) {
        return WorkflowViews.WorkflowView.from(service.resume(id));
    }
}
