package com.github.highcumontoa.workfloworchestrationjava.web;

import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowDefinition;
import com.github.highcumontoa.workfloworchestrationjava.model.WorkflowSnapshot;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowService;
import org.springframework.web.bind.annotation.*;

/** REST：提交 / 查询 / 恢复。 */
@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowService workflowService;

    public WorkflowController(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @PutMapping
    public WorkflowSnapshot submit(@RequestBody WorkflowDefinition definition) {
        return workflowService.submit(definition);
    }

    @GetMapping("/{workflowId}")
    public WorkflowSnapshot query(@PathVariable String workflowId) {
        return workflowService.query(workflowId);
    }

    @PostMapping("/{workflowId}/resume")
    public WorkflowSnapshot resume(@PathVariable String workflowId) {
        return workflowService.resume(workflowId);
    }
}
