package weareadaptive.com.cluster.services.oms;

import weareadaptive.com.cluster.services.oms.util.ExecutionResult;
import weareadaptive.com.cluster.services.oms.util.Order;
import weareadaptive.com.cluster.services.oms.util.Side;
import weareadaptive.com.cluster.services.oms.util.Status;

import java.util.HashSet;
import java.util.NavigableSet;
import java.util.TreeSet;

public class OrderbookImpl implements IOrderbook
{
    private long currentOrderId = 0;
    private final HashSet<Long> activeIds = new HashSet<>();
    private final TreeSet<Order> asks = new TreeSet<>();
    private final TreeSet<Order> bids = new TreeSet<>();

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

        if ((orderList.isEmpty() || orderList.first().getPrice() < newOrder.getPrice())) {
            return findMatchingOrders(newOrder, orderList, otherSideList);
        }
        else {
            orderList.add(newOrder);
            activeIds.add(newOrder.getOrderId());
            return new ExecutionResult(newOrder.getOrderId(), Status.RESTING);
        }
    }

    private ExecutionResult findMatchingOrders(Order newOrder, TreeSet<Order> orderList, TreeSet<Order> otherSideList)
    {
        long totalSize = newOrder.getSize();
        final var iter = otherSideList.iterator();
        while (iter.hasNext())
        {
            var orderFromIterator = iter.next();
            if (orderList == asks)
            {
                if (orderFromIterator.getPrice() < newOrder.getPrice()) continue;
            }
            else if (orderFromIterator.getPrice() > newOrder.getPrice()) continue;
            long sizeFulfilled;
            var orderSize = orderFromIterator.getSize();
            if (orderSize > totalSize)
            {
                sizeFulfilled = totalSize;
                orderFromIterator.setSize(orderSize - sizeFulfilled);
            } else {
                activeIds.remove(orderFromIterator.getOrderId());
                iter.remove();
                sizeFulfilled = orderSize;
            }
            totalSize -= sizeFulfilled;
            if (totalSize == 0) break;
        }

        if (totalSize == 0) return new ExecutionResult(newOrder.getOrderId(), Status.FILLED);

        orderList.add(newOrder);
        activeIds.add(newOrder.getOrderId());
        if (totalSize == newOrder.getSize()) return new ExecutionResult(newOrder.getOrderId(), Status.RESTING);
        else return new ExecutionResult(newOrder.getOrderId(), Status.PARTIAL);
    }

    /**
     * * Implement Cancel Order logic
     *  - Cancels order provided the orderId
     *  - Returns orderId and status (CANCELLED)
     */
    @Override
    public ExecutionResult cancelOrder(long orderId) {
        if (!activeIds.contains(orderId)) {
            return new ExecutionResult(orderId, Status.NONE);
        }
        activeIds.remove(orderId);
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

    public NavigableSet<Order> getAsksPriceAscending() {
        return asks.descendingSet();
    }

    public HashSet<Long> getActiveIds()
    {
        return activeIds;
    }

    public long getCurrentOrderId()
    {
        return currentOrderId;
    }

    public TreeSet<Order> getAsks()
    {
        return asks;
    }

    public TreeSet<Order> getBids()
    {
        return bids;
    }

    public void setCurrentOrderId(long currentOrderId)
    {
        this.currentOrderId = currentOrderId;
    }
}
