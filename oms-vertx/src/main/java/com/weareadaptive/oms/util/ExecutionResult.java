package com.weareadaptive.oms.util;

public class ExecutionResult {
    long orderId;
    Status status;

    public ExecutionResult(long orderId, Status status) {
        this.orderId = orderId;
        this.status = status;
    }

    public long getOrderId() {
        return orderId;
    }

    public Status getStatus() {
        return status;
    }
}
