package com.weareadaptive.gateway.codec;

import com.weareadaptive.cluster.services.oms.util.Method;
import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.cluster.services.util.ServiceName;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static com.weareadaptive.util.CodecConstants.*;

public class Encoder
{
    private final static MutableDirectBuffer headerBuffer = new UnsafeBuffer(ByteBuffer.allocate(HEADER_SIZE));
    private final static MutableDirectBuffer orderBuffer = new UnsafeBuffer(ByteBuffer.allocate(ORDER_REQUEST_SIZE));
    private final static MutableDirectBuffer orderIdBuffer = new UnsafeBuffer(ByteBuffer.allocate(ID_SIZE));
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
}
