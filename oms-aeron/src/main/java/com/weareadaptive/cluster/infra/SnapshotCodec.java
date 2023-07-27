package com.weareadaptive.cluster.infra;

import com.weareadaptive.cluster.services.oms.OrderbookImpl;
import com.weareadaptive.cluster.services.oms.util.Order;
import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SnapshotCodec
{
    public final static int END_OF_SNAPSHOT_MARKER_SIZE = Byte.BYTES;
    public final static DirectBuffer endOfSnapshotMarker = new UnsafeBuffer(ByteBuffer.allocateDirect(END_OF_SNAPSHOT_MARKER_SIZE).put((byte) -1));
    public static OrderbookImpl orderbook = new OrderbookImpl();
    public static int ORDER_SIZE = Long.BYTES + Double.BYTES + Long.BYTES;
    private static MutableDirectBuffer buffer;

    public static DirectBuffer encodeOrderbookState(TreeSet<Order> asks, TreeSet<Order> bids, long currentOrderId)
    {
        int totalAsksSize = Integer.BYTES + calculateOrdersSize(asks);
        int totalBidsSize = Integer.BYTES + calculateOrdersSize(bids);
        int bufferSize = totalAsksSize + totalBidsSize + Long.BYTES + END_OF_SNAPSHOT_MARKER_SIZE;

        buffer = new ExpandableArrayBuffer(bufferSize);
        int pos = 0;

        encodeOrdersToBuffer(asks, buffer, pos);
        pos += totalAsksSize;

        encodeOrdersToBuffer(bids, buffer, pos);
        pos += totalBidsSize;

        buffer.putLong(pos, currentOrderId);
        pos += Long.BYTES;

        buffer.putBytes(pos, endOfSnapshotMarker, 0, END_OF_SNAPSHOT_MARKER_SIZE);

        return buffer;
    }

    public static Order decodeOrder(int position, DirectBuffer buffer) {
        int pos = position;
        long orderId = buffer.getLong(pos);
        pos += Long.BYTES;
        double price = buffer.getDouble(pos);
        pos += Double.BYTES;
        long size = buffer.getLong(pos);
        pos += Long.BYTES;
        return new Order(orderId, price, size);
    }

    private static int calculateOrdersSize(TreeSet<Order> orders)
    {
        return orders.size() * ORDER_SIZE;
    }

    private static void encodeOrdersToBuffer(SortedSet<Order> orders, MutableDirectBuffer buffer, int pos)
    {
        int newPos = pos;
        buffer.putInt(newPos, orders.size());
        pos += Integer.BYTES;
        for (Order order : orders)
        {
            buffer.putLong(pos, order.getOrderId());
            pos += Long.BYTES;
            buffer.putDouble(pos, order.getPrice());
            pos += Double.BYTES;
            buffer.putLong(pos, order.getSize());
            pos += Long.BYTES;
        }
    }

    public static OrderbookImpl decodeOrderbookState(Image snapshotImage) {
        if (snapshotImage == null) {
            orderbook.reset();
            return orderbook;
        }

        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            orderbook = processOrderbookSnapshot(buffer, offset, length);
        };

        while (true) {
            int fragmentsRead = snapshotImage.poll(fragmentHandler, Integer.MAX_VALUE);
            if (fragmentsRead == 0) {
                break;
            }
        }

        return orderbook;
    }

    public static OrderbookImpl processOrderbookSnapshot(DirectBuffer buffer, int offset, int length)
    {
        int pos = offset;

        TreeSet<Order> asks = decodeOrders(pos, buffer);
        pos += Integer.BYTES + (asks.size() * ORDER_SIZE);

        TreeSet<Order> bids = decodeOrders(pos, buffer);
        pos += Integer.BYTES + (bids.size() * ORDER_SIZE);

        long currentOrderId = decodeCurrentOrderId(pos, buffer);

        HashSet<Long> activeIds = Stream.concat(asks.stream(), bids.stream()).map(Order::getOrderId)
                .collect(Collectors.toCollection(HashSet::new));

        orderbook.setAsks(asks);
        orderbook.setBids(bids);
        orderbook.setActiveIds(activeIds);
        orderbook.setCurrentOrderId(currentOrderId);

        return orderbook;
    }

    private static TreeSet<Order> decodeOrders(int pos, DirectBuffer buffer)
    {
            TreeSet<Order> orders = new TreeSet<>();
            int currentPosition = pos;
            int totalNumberOfOrders = buffer.getInt(currentPosition);
            currentPosition += Integer.BYTES;
            for (int i = 0; i < totalNumberOfOrders; i++) {
                orders.add(decodeOrder(currentPosition, buffer));
                currentPosition += ORDER_SIZE;
            }
        return orders;
    }

    private static long decodeCurrentOrderId(int pos, DirectBuffer buffer)
    {
        return buffer.getLong(pos);
    }


}