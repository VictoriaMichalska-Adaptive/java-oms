package com.weareadaptive.util;

import com.weareadaptive.cluster.services.util.ServiceName;
import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Method;
import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.cluster.services.oms.util.Status;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class Decoder
{
    public final static int METHOD_NAME_SIZE = Byte.BYTES;
    public final static int SERVICE_NAME_SIZE = Byte.BYTES;
    public final static int HEADER_SIZE = Byte.BYTES + Byte.BYTES + Long.BYTES;
    public final static int ORDER_SIZE = Double.BYTES + Long.BYTES + Byte.BYTES;
    public final static int ID_SIZE = Long.BYTES;
    public final static int EXECUTION_RESULT_SIZE = Long.BYTES + Long.BYTES + Byte.BYTES;
    public final static int SUCCESS_MESSAGE_SIZE = Long.BYTES + Byte.BYTES;
    public final static MutableDirectBuffer headerBuffer = new UnsafeBuffer(ByteBuffer.allocate(HEADER_SIZE));
    public final static MutableDirectBuffer orderBuffer = new UnsafeBuffer(ByteBuffer.allocate(ORDER_SIZE));
    public final static MutableDirectBuffer orderIdBuffer = new UnsafeBuffer(ByteBuffer.allocate(ID_SIZE));

    public static MutableDirectBuffer encodeOMSHeader(final ServiceName serviceName,
                                         final Method method, final long messageId) {
        int pos = 0;
        headerBuffer.putByte(pos, serviceName.getByte());
        pos += Byte.BYTES;

        headerBuffer.putByte(pos, method.getByte());
        pos += Byte.BYTES;

        headerBuffer.putLong(pos, messageId);

        return headerBuffer;
    }

    public static MutableDirectBuffer encodeId(long orderId) {
        orderIdBuffer.putLong(0, orderId);
        return orderIdBuffer;
    }

    public static MutableDirectBuffer encodeOrderRequest(double price, long size, Side side) {
        int pos = 0;
        orderBuffer.putDouble(pos, price);
        pos += Double.BYTES;
        orderBuffer.putLong(pos, size);
        pos += Long.BYTES;
        orderBuffer.putByte(pos, side.getByte());

        return orderBuffer;
    }

    public static OrderRequest decodeOrderRequest(final DirectBuffer buffer, final int offset) {
        int position = offset;

        double price = buffer.getDouble(position);
        position += Double.BYTES;

        long size = buffer.getLong(position);
        position += Long.BYTES;

        byte sideByte = buffer.getByte(position);
        Side side = Side.fromByteValue(sideByte);

        return new OrderRequest(price, size, side);
    }

    public static ExecutionResult decodeExecutionResult(final DirectBuffer buffer, final int offset) {
        int position = offset;
        final long orderId = buffer.getLong(position);
        position += Long.BYTES;
        final Status status = Status.fromByteValue(buffer.getByte(position));

        return new ExecutionResult(orderId, status);
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

    public static MutableDirectBuffer encodeSuccessMessage(Long messageId) {
        MutableDirectBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocate(SUCCESS_MESSAGE_SIZE));

        int currentPos = 0;
        responseBuffer.putLong(currentPos, messageId);
        currentPos += Long.BYTES;
        responseBuffer.putByte(currentPos, Status.SUCCESS.getByte());

        return responseBuffer;
    }

    public static Status decodeStatus(final DirectBuffer buffer, final int offset) {
        return Status.fromByteValue(buffer.getByte(offset));
    }

    public static MutableDirectBuffer encodeExecutionResult(Long messageId, ExecutionResult executionResult) {
        MutableDirectBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocate(EXECUTION_RESULT_SIZE));

        int currentPos = 0;
        responseBuffer.putLong(currentPos, messageId);
        currentPos += Long.BYTES;
        responseBuffer.putLong(currentPos, executionResult.getOrderId());
        currentPos += Long.BYTES;
        responseBuffer.putByte(currentPos, executionResult.getStatus().getByte());

        return responseBuffer;
    }
}
