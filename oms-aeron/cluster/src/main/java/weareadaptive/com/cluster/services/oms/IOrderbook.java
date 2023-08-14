package weareadaptive.com.cluster.services.oms;

import weareadaptive.com.cluster.services.oms.util.ExecutionResult;
import weareadaptive.com.cluster.services.oms.util.Side;

public interface IOrderbook {
    ExecutionResult placeOrder(double price, long size, Side side);
    ExecutionResult cancelOrder(long orderId);
    void clear();
    void reset();
}
