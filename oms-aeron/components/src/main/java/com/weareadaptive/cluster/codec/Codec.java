package com.weareadaptive.cluster.codec;

import com.weareadaptive.cluster.services.oms.util.*;
import com.weareadaptive.cluster.services.util.CustomHeader;
import com.weareadaptive.cluster.services.util.ServiceName;
import com.weareadaptive.cluster.services.util.OrderRequestCommand;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.TreeSet;

import static com.weareadaptive.util.CodecConstants.*;

public class Codec
{
    private final static MutableDirectBuffer executionResultBuffer = new UnsafeBuffer(ByteBuffer.allocate(EXECUTION_RESULT_SIZE));
    private final static MutableDirectBuffer successMessageBuffer = new UnsafeBuffer(ByteBuffer.allocate(SUCCESS_MESSAGE_SIZE));
    private final static MutableDirectBuffer orderIdResponseBuffer = new UnsafeBuffer(ByteBuffer.allocate(ORDER_ID_RESPONSE_SIZE));
    private static MutableDirectBuffer ordersBuffer;
    private final static CustomHeader customHeader = new CustomHeader();
    private static final OrderRequestCommand orderRequestCommand = new OrderRequestCommand();

    public static OrderRequestCommand decodeOrderRequest(final DirectBuffer buffer, final int offset) {
        int position = offset;

        double price = buffer.getDouble(position);
        position += Double.BYTES;

        long size = buffer.getLong(position);
        position += Long.BYTES;

        byte sideByte = buffer.getByte(position);
        Side side = Side.fromByteValue(sideByte);

        return orderRequestCommand.setProperties(price, size, side);
    }

    public static long decodeLongId(final DirectBuffer buffer, final int offset) {
        return buffer.getLong(offset);
    }

    public static Method decodeMethod(final DirectBuffer buffer, final int offset) {
        return Method.fromByteValue(buffer.getByte(offset));
    }

    public static ServiceName decodeServiceName(final DirectBuffer buffer, final int offset) {
        return ServiceName.fromByteValue(buffer.getByte(offset));
    }

    public static CustomHeader decodeOMSHeader(final DirectBuffer buffer, final int offset) {
        int position = offset;
        final ServiceName serviceName = ServiceName.fromByteValue(buffer.getByte(position));
        position += Byte.BYTES;
        final Method method = Method.fromByteValue(buffer.getByte(position));
        position += Byte.BYTES;
        final long id = buffer.getLong(position);

        return customHeader.setProperties(serviceName, method, id);
    }

    public static MutableDirectBuffer encodeSuccessMessage(Long messageId) {
        int currentPos = 0;
        successMessageBuffer.putLong(currentPos, messageId);
        currentPos += Long.BYTES;
        successMessageBuffer.putByte(currentPos, Status.SUCCESS.getByte());

        return successMessageBuffer;
    }

    public static MutableDirectBuffer encodeExecutionResult(Long messageId, ExecutionResult executionResult) {
        int currentPos = 0;
        executionResultBuffer.putLong(currentPos, messageId);
        currentPos += Long.BYTES;
        executionResultBuffer.putLong(currentPos, executionResult.getOrderId());
        currentPos += Long.BYTES;
        executionResultBuffer.putByte(currentPos, executionResult.getStatus().getByte());

        return executionResultBuffer;
    }

    public static DirectBuffer encodeOrders(long correlationId, TreeSet<Order> orders) {
        ordersBuffer = new ExpandableDirectByteBuffer(Long.BYTES + (orders.size() * ORDER_SIZE));
        int pos = 0;
        ordersBuffer.putLong(pos, correlationId);
        pos += Long.BYTES;
        // todo: avoid code duplication without losing separation of concerns
        ordersBuffer.putInt(pos, orders.size());
        pos += Integer.BYTES;
        for (Order order : orders)
        {
            ordersBuffer.putLong(pos, order.getOrderId());
            pos += Long.BYTES;
            ordersBuffer.putDouble(pos, order.getPrice());
            pos += Double.BYTES;
            ordersBuffer.putLong(pos, order.getSize());
            pos += Long.BYTES;
        }
        return ordersBuffer;
    }

    public static DirectBuffer encodeOrderIdResponse(long correlationId, long currentOrderId)
    {
        orderIdResponseBuffer.putLong(0, correlationId);
        orderIdResponseBuffer.putLong(Long.BYTES, currentOrderId);
        return orderIdResponseBuffer;
    }
}
