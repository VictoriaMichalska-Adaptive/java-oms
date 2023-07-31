package com.weareadaptive.gateway.codec;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Order;
import com.weareadaptive.cluster.services.oms.util.Status;
import org.agrona.DirectBuffer;

import java.util.TreeSet;

import static com.weareadaptive.util.CodecConstants.ORDER_SIZE;

public class Decoder
{
    private static final ExecutionResult executionResult = new ExecutionResult();
    public static long decodeLongId(final DirectBuffer buffer, final int offset) {
        return buffer.getLong(offset);
    }

    public static Status decodeStatus(final DirectBuffer buffer, final int offset) {
        return Status.fromByteValue(buffer.getByte(offset));
    }

    public static ExecutionResult decodeExecutionResult(final DirectBuffer buffer, final int offset) {
        int position = offset;
        final long orderId = buffer.getLong(position);
        position += Long.BYTES;
        final Status status = Status.fromByteValue(buffer.getByte(position));

        executionResult.setOrderId(orderId);
        executionResult.setStatus(status);
        return executionResult;
    }

    public static TreeSet<Order> decodeOrders(int pos, DirectBuffer buffer)
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

    public static long decodeOrderIdResponse(int position, DirectBuffer buffer) {
        return buffer.getLong(position);
    }
}
