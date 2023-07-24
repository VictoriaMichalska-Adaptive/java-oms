package com.weareadaptive.oms;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Method;
import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.cluster.services.oms.util.Status;
import com.weareadaptive.cluster.services.util.ServiceName;
import com.weareadaptive.cluster.services.util.OrderRequestCommand;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

import static com.weareadaptive.util.Decoder.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DecoderTest
{
    public static void printBufferContents(MutableDirectBuffer buffer) {
        byte[] bytes = new byte[buffer.capacity()];
        buffer.getBytes(0, bytes);

        System.out.println("Buffer Contents:");
        for (int i = 0; i < buffer.capacity(); i++) {
            System.out.print(bytes[i] + " ");
        }
        System.out.println();
        System.out.println("Buffer Contents (in binary):");
        for (int i = 0; i < buffer.capacity(); i++) {
            String binaryString = String.format("%8s", Integer.toBinaryString(buffer.getByte(i) & 0xFF)).replace(' ', '0');
            System.out.print(binaryString + " ");
        }
        System.out.println();
    }

    @Test
    public void encodeAndDecodeOMSHeader() {
        MutableDirectBuffer encodedHeader = encodeOMSHeader(ServiceName.OMS, Method.CANCEL, 12L);
        ServiceName serviceName = decodeServiceName(encodedHeader, 0);
        Method method = decodeMethod(encodedHeader, SERVICE_NAME_SIZE);
        long messageId = decodeLongId(encodedHeader, SERVICE_NAME_SIZE + METHOD_NAME_SIZE);

        assertEquals(ServiceName.OMS, serviceName);
        assertEquals(Method.CANCEL, method);
        assertEquals(12L, messageId);
    }

    @Test
    public void encodeAndDecodeOrderRequest() {
        MutableDirectBuffer buffer = encodeOrderRequest(10.45, 20, Side.BID);
        OrderRequestCommand orderRequestCommand = decodeOrderRequest(buffer, 0);

        assertEquals(10.45, orderRequestCommand.getPrice());
        assertEquals(20, orderRequestCommand.getSize());
        assertEquals(Side.BID, orderRequestCommand.getSide());
    }

    @Test
    public void encodeAndDecodeExecutionResult() {
        MutableDirectBuffer buffer = encodeExecutionResult(43L, new ExecutionResult(30, Status.CANCELLED));
        long messageId = decodeLongId(buffer, 0);
        ExecutionResult executionResult = decodeExecutionResult(buffer, ID_SIZE);

        assertEquals(43L, messageId);
        assertEquals(30, executionResult.getOrderId());
        assertEquals(Status.CANCELLED, executionResult.getStatus());
    }

    @Test
    public void encodeAndDecodeSuccessMessage() {
        MutableDirectBuffer buffer = encodeSuccessMessage(104L);
        long messageId = decodeLongId(buffer, 0);
        Status status = decodeStatus(buffer, ID_SIZE);

        assertEquals(104L, messageId);
        assertEquals(Status.SUCCESS, status);
    }

    @Test
    public void encodeAndDecodeId() {
        MutableDirectBuffer buffer = encodeId(132);
        final var id = decodeLongId(buffer, 0);
        assertEquals(132, id);
    }
}
