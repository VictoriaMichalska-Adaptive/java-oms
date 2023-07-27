package com.weareadaptive.gateway.codec;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Status;
import org.agrona.DirectBuffer;

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
}
