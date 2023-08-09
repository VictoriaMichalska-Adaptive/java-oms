package com.weareadaptive.cluster.services.infra;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Order;
import com.weareadaptive.cluster.services.oms.util.Status;
import com.weareadaptive.sbe.*;
import io.aeron.cluster.service.ClientSession;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.TreeSet;

public class ClusterClientResponderImpl implements ClusterClientResponder
{
    private final Logger LOGGER = LoggerFactory.getLogger(ClusterClientResponderImpl.class);
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ExecutionResultEncoder executionResultEncoder = new ExecutionResultEncoder();
    private final SuccessMessageEncoder successMessageEncoder = new SuccessMessageEncoder();
    private final OrderIdEncoder orderIdEncoder = new OrderIdEncoder();
    private final OrderEncoder orderEncoder = new OrderEncoder();
    private final EndOfOrdersEncoder endOfOrdersEncoder = new EndOfOrdersEncoder();
    @Override
    public void onExecutionResult(ClientSession session, long correlationId, ExecutionResult executionResult)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + ExecutionResultEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));
        messageHeaderEncoder.wrap(directBuffer, 0);
        messageHeaderEncoder.correlationId(correlationId);

        executionResultEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);
        executionResultEncoder.orderId(executionResult.getOrderId());
        executionResultEncoder.status(executionResult.getStatus().getByte());
        while (session.offer(directBuffer, 0, encodedLength) < 0);
    }

    @Override
    public void onSuccessMessage(ClientSession session, long correlationId)
    {
        final int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + SuccessMessageEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));
        messageHeaderEncoder.wrap(directBuffer, 0);
        messageHeaderEncoder.correlationId(correlationId);

        successMessageEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);
        successMessageEncoder.status(Status.SUCCESS.getByte());
        while (session.offer(directBuffer, 0, encodedLength) < 0);
    }

    @Override
    public void onOrders(ClientSession session, long correlationId, TreeSet<Order> orders)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + OrderEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));
        messageHeaderEncoder.wrap(directBuffer, 0);
        messageHeaderEncoder.correlationId(correlationId);
        orderEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);

        for (Order order : orders)
        {
            orderEncoder.orderId(order.getOrderId());
            orderEncoder.price(order.getPrice());
            orderEncoder.size(order.getSize());
            while (session.offer(directBuffer, 0, encodedLength) < 0);
        }

        // todo: figure out what wrap and wrapAndApplyHeader actually do with the offset
        MutableDirectBuffer endOfOrdersBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MessageHeaderEncoder.ENCODED_LENGTH));
        messageHeaderEncoder.wrap(endOfOrdersBuffer, 0);
        messageHeaderEncoder.correlationId(correlationId);
        
        endOfOrdersEncoder.wrapAndApplyHeader(endOfOrdersBuffer, 0, messageHeaderEncoder);
        while (session.offer(endOfOrdersBuffer, 0, MessageHeaderEncoder.ENCODED_LENGTH) < 0);
    }

    @Override
    public void onOrderId(ClientSession session, long correlationId, long currentOrderId)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + OrderIdEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));
        messageHeaderEncoder.wrap(directBuffer, 0);
        messageHeaderEncoder.correlationId(correlationId);

        orderIdEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);
        orderIdEncoder.orderId(currentOrderId);
        while (session.offer(directBuffer, 0, encodedLength) < 0);
    }
}
