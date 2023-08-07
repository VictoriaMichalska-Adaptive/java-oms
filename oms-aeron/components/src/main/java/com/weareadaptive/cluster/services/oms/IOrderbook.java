package com.weareadaptive.cluster.services.oms;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Side;

public interface IOrderbook {
    ExecutionResult placeOrder(double price, long size, Side side);
    ExecutionResult cancelOrder(long orderId);
    void clear();
    void reset();
}
