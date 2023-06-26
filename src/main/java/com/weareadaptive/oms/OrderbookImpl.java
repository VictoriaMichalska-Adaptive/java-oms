package com.weareadaptive.oms;

import com.weareadaptive.oms.util.ExecutionResult;
import com.weareadaptive.oms.util.Order;
import com.weareadaptive.oms.util.Side;
import com.weareadaptive.oms.util.Status;

import java.util.ArrayList;

public class OrderbookImpl implements IOrderbook{
    long currentOrderId = 0;
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
        ArrayList<Order> orderList = null;
        ArrayList<Order> otherSideList = null;
        switch(side) {
            case ASK:
                orderList = asks;
                otherSideList = bids;
            case BID:
                orderList = bids;
                otherSideList = asks;
        }

        addOrderToSortedList(side, newOrder);

        if (otherSideList == null || otherSideList.isEmpty()) {
            return new ExecutionResult(id, Status.RESTING);
        }
        else if (!orderList.isEmpty()) {
            if (orderList.get(0).getPrice() < price)
            {
                return new ExecutionResult(id, Status.RESTING);
            }
            else {
                return new ExecutionResult(id, Status.FILLED);
            }
        }
        return new ExecutionResult(id, Status.RESTING);
    }

    private void addOrderToSortedList(Side side, Order newOrder) {
        final ArrayList<Order> orderList;
        switch(side) {
            case ASK -> orderList = asks;
            case BID -> orderList = bids;
            default -> throw new IllegalArgumentException(side.toString());
        }
        int index = orderList.size();

        // find the index to insert the newOrder by comparing the prices
        for (int i = 0; i < orderList.size(); i++) {
            if (newOrder.getPrice() > orderList.get(i).getPrice()) {
                index = i;
                break;
            }
        }

        // insert the newOrder at the determined index
        orderList.add(index, newOrder);
    }

    /**
     * * Implement Cancel Order logic
     *  - Cancels order provided the orderId
     *  - Returns orderId and status (CANCELLED, NONE)
     */
    @Override
    public ExecutionResult cancelOrder(long orderId) {
        return new ExecutionResult(0, Status.NONE);
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
