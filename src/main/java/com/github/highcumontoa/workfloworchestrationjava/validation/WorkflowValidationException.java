package com.github.highcumontoa.workfloworchestrationjava.validation;

/** 工作流登记校验失败时抛出，原因可通过 {@link #reason} 区分。 */
public class WorkflowValidationException extends RuntimeException {

    private final RejectReason reason;

    public WorkflowValidationException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
