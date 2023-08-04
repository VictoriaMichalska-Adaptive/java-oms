package com.weareadaptive.oms;

import com.weareadaptive.cluster.services.oms.util.*;
import com.weareadaptive.cluster.services.util.CustomHeader;
import com.weareadaptive.cluster.services.util.OrderRequestCommand;
import com.weareadaptive.cluster.services.util.ServiceName;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

import java.util.TreeSet;

import static com.weareadaptive.cluster.codec.Codec.*;
import static com.weareadaptive.gateway.codec.Decoder.decodeExecutionResult;
import static com.weareadaptive.gateway.codec.Decoder.decodeStatus;
import static com.weareadaptive.gateway.codec.Encoder.*;
import static com.weareadaptive.util.CodecConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CodecTest
{
    @Test
    public void encodeAndDecodeOMSHeader() {
        MutableDirectBuffer encodedHeader = encodeOMSHeader(ServiceName.OMS, Method.CANCEL, 12L);
        ServiceName serviceName = decodeServiceName(encodedHeader, 0);
        Method method = decodeMethod(encodedHeader, SERVICE_NAME_SIZE);
        long messageId = decodeLongId(encodedHeader, SERVICE_NAME_SIZE + METHOD_NAME_SIZE);
        CustomHeader customHeader = decodeOMSHeader(encodedHeader, 0);

        assertEquals(ServiceName.OMS, customHeader.getServiceName());
        assertEquals(Method.CANCEL, customHeader.getMethod());
        assertEquals(12L, customHeader.getMessageId());

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

    private TreeSet<Order> createOrders(int start, int end)
    {
        TreeSet<Order> treeSet = new TreeSet<>();
        for (int val = start; val <= end; val++)
        {
            treeSet.add(new Order(val, val, val));
        }
        return treeSet;
    }
}
