package com.weareadaptive.cluster.services.infra;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Order;
import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.weareadaptive.cluster.codec.Codec.*;
import static com.weareadaptive.util.CodecConstants.*;

public class ClusterClientResponderImpl implements ClusterClientResponder
{
    private final Logger LOGGER = LoggerFactory.getLogger(ClusterClientResponderImpl.class);
    @Override
    public void onExecutionResult(ClientSession session, long correlationId, ExecutionResult executionResult)
    {
        while (session.offer(encodeExecutionResult(correlationId, executionResult), 0, EXECUTION_RESULT_SIZE) < 0);
    }

    @Override
    public void onSuccessMessage(ClientSession session, long correlationId)
    {
        while (session.offer(encodeSuccessMessage(correlationId), 0, SUCCESS_MESSAGE_SIZE) < 0);
    }

    @Override
    public void onOrders(ClientSession session, long correlationId, TreeSet<Order> orders)
    {
        LOGGER.info("Sending back orders with the following IDs: " + orders.stream().map(x -> String.valueOf(x.getOrderId())).collect(Collectors.joining(", ")));
        final DirectBuffer buffer = encodeOrders(correlationId, orders);
        while (session.offer(buffer, 0, buffer.capacity()) < 0);
    }

    @Override
    public void onOrderId(ClientSession session, long correlationId, long currentOrderId)
    {
        while (session.offer(encodeOrderIdResponse(correlationId, currentOrderId), 0, ORDER_ID_RESPONSE_SIZE) < 0);
    }
}
