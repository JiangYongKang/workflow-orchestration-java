package com.github.highcumontoa.workfloworchestrationjava.validation;

import com.github.highcumontoa.workfloworchestrationjava.model.RejectReason;

/** 登记校验失败异常，携带可区分的拒绝原因。 */
public class ValidationException extends RuntimeException {
    private final RejectReason reason;

    public ValidationException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() { return reason; }
}
