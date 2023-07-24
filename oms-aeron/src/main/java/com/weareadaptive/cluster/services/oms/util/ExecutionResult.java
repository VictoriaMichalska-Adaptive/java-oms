package com.weareadaptive.cluster.services.oms.util;

public class ExecutionResult {
    long orderId;
    Status status;

    public ExecutionResult(long orderId, Status status) {
        this.orderId = orderId;
        this.status = status;
    }

    public ExecutionResult() {
    }

    public void setOrderId(long orderId)
    {
        this.orderId = orderId;
    }
    public void setStatus(Status status)
    {
        this.status = status;
    }
    public long getOrderId() {
        return orderId;
    }
    public Status getStatus() {
        return status;
    }
}
