package com.weareadaptive.oms;

import com.weareadaptive.oms.util.ExecutionResult;
import com.weareadaptive.oms.util.Order;
import com.weareadaptive.oms.util.Side;
import com.weareadaptive.oms.util.Status;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class OrderbookImpl implements IOrderbook{
    long currentOrderId = 0;
    HashMap<Status, HashSet<Long>> allStatuses = new HashMap<>();
    ArrayList<Order> asks = new ArrayList<>();
    ArrayList<Order> bids = new ArrayList<>();

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
        ArrayList<Order> orderList;
        ArrayList<Order> otherSideList;
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
        if (orderList.isEmpty() || otherSideList.isEmpty()) {
            return findMatchingOrders(newOrder, orderList, otherSideList);
        }
        else if (orderList.get(0).getPrice() < newOrder.getPrice())
        {
            return findMatchingOrders(newOrder, orderList, otherSideList);
        }
        else
        {
            int index = Collections.binarySearch(orderList, newOrder);
            if (index < 0) index = ~index;
            orderList.add(index, newOrder);

            return new ExecutionResult(id, Status.RESTING);
        }
    }

    private ExecutionResult findMatchingOrders(Order newOrder, ArrayList<Order> orderList, ArrayList<Order> otherSideList)
    {
        if (otherSideList.isEmpty()) {
            orderList.add(0, newOrder);
            return addToStatusHash(newOrder.getOrderId(), Status.RESTING);
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
                    order.setSize(orderSize - totalSize);
                    sizeFulfilled = totalSize;
                }
                else {
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

    private ExecutionResult addToStatusHash(long orderId, Status status)
    {
        if (allStatuses.get(status) == null) {
            final var restingHashSet = new HashSet<Long>();
            allStatuses.put(status, restingHashSet);
        }
        else { allStatuses.get(status).add(orderId); }
        return new ExecutionResult(orderId, status);
    }

    /**
     * * Implement Cancel Order logic
     *  - Cancels order provided the orderId
     *  - Returns orderId and status (CANCELLED, NONE)
     */
    // todo: actually cancel the order by moving it from the hashlist lol
    @Override
    public ExecutionResult cancelOrder(long orderId) {
        return new ExecutionResult(orderId, Status.CANCELLED);
    }

    /**
     * * Implement Clear orderbook logic
     *  - Should clear all orders
     *  - Retain orderId state
     */
    @Override
    public void clear() {
    }

    /**
     * * Implement Reset orderbook logic
     *  - Should clear all orders
     *  - Reset orderId state
     *  - All states should be reset
     */
    @Override
    public void reset() {
    }
}
