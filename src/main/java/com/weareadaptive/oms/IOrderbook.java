package com.weareadaptive.oms;

import com.weareadaptive.oms.util.ExecutionResult;
import com.weareadaptive.oms.util.Side;

public interface IOrderbook {
    ExecutionResult placeOrder(double price, long size, Side side);
    ExecutionResult cancelOrder(long orderId);
    void clear();
    void reset();
}
