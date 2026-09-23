package com.github.highcumontoa.workfloworchestrationjava.api;

import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowConflictException;
import com.github.highcumontoa.workfloworchestrationjava.service.WorkflowNotFoundException;
import com.github.highcumontoa.workfloworchestrationjava.validation.WorkflowValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 领域异常到 HTTP 状态与可区分 reason 的映射。 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(WorkflowValidationException.class)
    public ResponseEntity<WorkflowViews.ErrorView> handleValidation(WorkflowValidationException e) {
        // 循环依赖 / 未知依赖 / 同名任务 / 非法请求统一 400，body 中的 reason 用于区分原因
        return ResponseEntity.badRequest()
                .body(new WorkflowViews.ErrorView(e.getReason().name(), e.getMessage()));
    }

    @ExceptionHandler(WorkflowConflictException.class)
    public ResponseEntity<WorkflowViews.ErrorView> handleConflict(WorkflowConflictException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new WorkflowViews.ErrorView("DEFINITION_CONFLICT", e.getMessage()));
    }

    @ExceptionHandler(WorkflowNotFoundException.class)
    public ResponseEntity<WorkflowViews.ErrorView> handleNotFound(WorkflowNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new WorkflowViews.ErrorView("WORKFLOW_NOT_FOUND", e.getMessage()));
    }
}
