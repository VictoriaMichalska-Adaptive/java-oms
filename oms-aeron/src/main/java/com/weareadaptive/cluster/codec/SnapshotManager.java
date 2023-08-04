package com.weareadaptive.cluster.codec;

import com.weareadaptive.cluster.services.oms.OrderbookImpl;
import com.weareadaptive.cluster.services.oms.util.Order;
import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.cluster.services.util.SnapshotHeader;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.weareadaptive.util.CodecConstants.ORDER_SIZE;

public class SnapshotManager implements FragmentHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotManager.class);
    private boolean snapshotFullyLoaded = false;
    private IdleStrategy idleStrategy;
    public OrderbookImpl orderbook = new OrderbookImpl();
    private final MutableDirectBuffer currentIdBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Byte.BYTES + Long.BYTES));
    private final MutableDirectBuffer endOfSnapshotBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Byte.BYTES));

    public void encodeOrderbookState(ExclusivePublication snapshotPublication, TreeSet<Order> asks, TreeSet<Order> bids, long currentOrderId)
    {
        LOGGER.info("Starting snapshot...");
        snapshotPublication.offer(encodeOrdersToBuffer(asks, Side.ASK));
        snapshotPublication.offer(encodeOrdersToBuffer(bids, Side.BID));
        snapshotPublication.offer(encodeCurrentIdsToBuffer(currentOrderId));
        snapshotPublication.offer(encodeEndOfSnapshotBuffer());
        LOGGER.info("Snapshot complete");
    }

    private DirectBuffer encodeEndOfSnapshotBuffer()
    {
        endOfSnapshotBuffer.putByte(0, SnapshotHeader.END_OF_SNAPSHOT.getByte());
        return endOfSnapshotBuffer;
    }

    private DirectBuffer encodeCurrentIdsToBuffer(long currentOrderId)
    {
        int position = 0;
        currentIdBuffer.putByte(position, SnapshotHeader.ORDER_IDS.getByte());
        position += Byte.BYTES;
        currentIdBuffer.putLong(position, currentOrderId);
        return currentIdBuffer;
    }


    public Order decodeOrder(int position, DirectBuffer buffer) {
        int pos = position;
        long orderId = buffer.getLong(pos);
        pos += Long.BYTES;
        double price = buffer.getDouble(pos);
        pos += Double.BYTES;
        long size = buffer.getLong(pos);
        pos += Long.BYTES;
        return new Order(orderId, price, size);
    }

    private MutableDirectBuffer encodeOrdersToBuffer(SortedSet<Order> orders, Side side)
    {
        MutableDirectBuffer ordersBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Byte.BYTES + Integer.BYTES + (ORDER_SIZE * orders.size())));
        int position = 0;
        SnapshotHeader snapshotHeader = side == Side.BID ? SnapshotHeader.BIDS : SnapshotHeader.ASKS;
        ordersBuffer.putByte(position, snapshotHeader.getByte());
        position += Byte.BYTES;
        ordersBuffer.putInt(position, orders.size());
        position += Integer.BYTES;
        for (Order order : orders)
        {
            ordersBuffer.putLong(position, order.getOrderId());
            position += Long.BYTES;
            ordersBuffer.putDouble(position, order.getPrice());
            position += Double.BYTES;
            ordersBuffer.putLong(position, order.getSize());
            position += Long.BYTES;
        }
        return ordersBuffer;
    }

    private TreeSet<Order> decodeOrders(int pos, DirectBuffer buffer)
    {
        TreeSet<Order> orders = new TreeSet<>();
        int currentPosition = pos;
        int totalNumberOfOrders = buffer.getInt(currentPosition);
        currentPosition += Integer.BYTES;
        for (int i = 0; i < totalNumberOfOrders; i++)
        {
            orders.add(decodeOrder(currentPosition, buffer));
            currentPosition += ORDER_SIZE;
        }
        return orders;
    }

    private static long decodeCurrentOrderId(int pos, DirectBuffer buffer)
    {
        return buffer.getLong(pos);
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
    {
        if (length < Byte.BYTES) {
            return;
        }

        final int updatedOffset = offset + Byte.BYTES;
        SnapshotHeader snapshotHeader = SnapshotHeader.fromByteValue(buffer.getByte(offset));
        switch (snapshotHeader) {
            case ASKS -> orderbook.setAsks(decodeOrders(updatedOffset, buffer));
            case BIDS -> orderbook.setBids(decodeOrders(updatedOffset, buffer));
            case ORDER_IDS -> orderbook.setCurrentOrderId(decodeCurrentOrderId(updatedOffset, buffer));
            case END_OF_SNAPSHOT -> snapshotFullyLoaded = true;
        }
    }

    public OrderbookImpl loadSnapshot(final Image snapshotImage)
    {
        LOGGER.info("Loading snapshot...");
        snapshotFullyLoaded = false;
        Objects.requireNonNull(idleStrategy, "Idle strategy must be set before loading snapshot");
        idleStrategy.reset();
        while (!snapshotImage.isEndOfStream())
        {
            idleStrategy.idle(snapshotImage.poll(this, 20));
        }

        if (!snapshotFullyLoaded)
        {
            LOGGER.warn("Snapshot load not completed; no end of snapshot marker found");
        }
        LOGGER.info("Snapshot load complete.");

        return orderbook;
    }

    public void setIdleStrategy(final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;
    }
}