package com.weareadaptive.oms;

import com.weareadaptive.oms.util.ExecutionResult;
import com.weareadaptive.oms.util.Order;
import com.weareadaptive.oms.util.Side;
import com.weareadaptive.oms.util.Status;

import java.util.HashSet;
import java.util.TreeSet;

public class OrderbookImpl implements IOrderbook{
    long currentOrderId = 0;
    HashSet<Long> activeIds = new HashSet<>();
    TreeSet<Order> asks = new TreeSet<>();
    TreeSet<Order> bids = new TreeSet<>();

    /**
     * * Implement Place Order logic
     *  - Resting orders if prices do not cross
     *  - Matching orders if prices do cross
     *  - Returns orderId and status (RESTING, PARTIAL, FILLED)
     */
    @Override
    public ExecutionResult placeOrder(double price, long size, Side side) {
        final var id = currentOrderId++;
        return fillOutOrder(side, id, price, size);
    }

    private ExecutionResult fillOutOrder(Side side, long id, double price, long size) {
        final var newOrder = new Order(id, price, size);
        TreeSet<Order> orderList;
        TreeSet<Order> otherSideList;

        switch (side)
        {
            case ASK ->
            {
                orderList = asks;
                otherSideList = bids;
            }
            case BID ->
            {
                orderList = bids;
                otherSideList = asks;
            }
            default -> throw new IllegalArgumentException();
        }
        if ((orderList.isEmpty() || otherSideList.isEmpty()) || (orderList.first().getPrice() < newOrder.getPrice())) {
            return findMatchingOrders(newOrder, orderList, otherSideList);
        }
        else {
            orderList.add(newOrder);

            return new ExecutionResult(id, Status.RESTING);
        }
    }

    private ExecutionResult findMatchingOrders(Order newOrder, TreeSet<Order> orderList, TreeSet<Order> otherSideList)
    {
        if (otherSideList.isEmpty()) {
            orderList.add(newOrder);
            activeIds.add(newOrder.getOrderId());
            return new ExecutionResult(newOrder.getOrderId(), Status.RESTING);
        }

        long totalSize = newOrder.getSize();
        final var iter = otherSideList.iterator();
        while (iter.hasNext())
        {
            var order = iter.next();
            if (order.getPrice() > newOrder.getPrice()) continue;
            long sizeFulfilled;
            if (totalSize > 0) {
                var orderSize = order.getSize();
                if (orderSize > totalSize) {
                    sizeFulfilled = totalSize;
                    order.setSize(orderSize - sizeFulfilled);
                }
                else {
                    removeIdFromSystem(order.getOrderId());
                    iter.remove();
                    sizeFulfilled = orderSize;
                }
                totalSize -= sizeFulfilled;
            }
            else break;
        }

        if (totalSize == 0) { return new ExecutionResult(newOrder.getOrderId(), Status.FILLED); }
        else if (totalSize == newOrder.getSize()) return new ExecutionResult(newOrder.getOrderId(), Status.RESTING);
        else { return new ExecutionResult(newOrder.getOrderId(), Status.PARTIAL); }
    }

    private void removeIdFromSystem(long orderId)
    {
        activeIds.remove(orderId);
    }

    /**
     * * Implement Cancel Order logic
     *  - Cancels order provided the orderId
     *  - Returns orderId and status (CANCELLED, NONE)
     */
    // todo: actually cancel the order by moving it from the hashlist lol
    @Override
    public ExecutionResult cancelOrder(long orderId) {
        if (!activeIds.contains(orderId)) {
            return new ExecutionResult(orderId, Status.NONE);
        }
        removeIdFromSystem(orderId);
        return new ExecutionResult(orderId, Status.CANCELLED);
    }

    /**
     * * Implement Clear orderbook logic
     *  - Should clear all orders
     *  - Retain orderId state
     */
    @Override
    public void clear() {
        asks.clear();
        bids.clear();
        activeIds.clear();
    }

    /**
     * * Implement Reset orderbook logic
     *  - Should clear all orders
     *  - Reset orderId state
     *  - All states should be reset
     */
    @Override
    public void reset() {
        clear();
        currentOrderId = 0;
    }
}
