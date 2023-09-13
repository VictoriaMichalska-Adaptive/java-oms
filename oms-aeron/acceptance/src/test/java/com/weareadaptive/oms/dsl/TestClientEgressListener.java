package com.weareadaptive.oms.dsl;

import com.weareadaptive.sbe.*;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import weareadaptive.com.cluster.services.oms.util.ExecutionResult;
import weareadaptive.com.cluster.services.oms.util.Order;
import weareadaptive.com.cluster.services.oms.util.Status;
import weareadaptive.com.gateway.exception.BadFieldException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestClientEgressListener implements EgressListener
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final TestClientCodec codec = new TestClientCodec();
    private final Map<Long, List<Order>> ordersRequests = new ConcurrentHashMap<>();
    private final HashMap<Long, List<Order>> receivedOrders = new HashMap<>();
    private final HashMap<Long, ExecutionResult> receivedExecutionResults = new HashMap<>();
    private final HashMap<Long, Status> receivedStatus = new HashMap<>();
    private final HashMap<Long, Long> receivedOrderId = new HashMap<>();

    @Override
    public void onMessage(long clusterSessionId, long timestamp, DirectBuffer buffer, int offset, int length, Header header)
    {
        int bufferOffset = offset;
        messageHeaderDecoder.wrap(buffer, bufferOffset);
        final int typeOfMessage = messageHeaderDecoder.templateId();
        final long correlationId = messageHeaderDecoder.correlationId();


        final int actingBlockLength = messageHeaderDecoder.blockLength();
        final int actingVersion = messageHeaderDecoder.version();
        bufferOffset += messageHeaderDecoder.encodedLength();

        route(buffer, bufferOffset, typeOfMessage, correlationId, actingBlockLength, actingVersion);
    }

    private void route(final DirectBuffer buffer, final int bufferOffset, final int typeOfMessage, final long correlationId,
                       final int actingBlockLength, final int actingVersion)
    {
        switch (typeOfMessage)
        {
            case OrderDecoder.TEMPLATE_ID ->
                    addOrderToCollectionOfAllOrders(correlationId, buffer, bufferOffset, actingBlockLength, actingVersion);
            case SuccessMessageDecoder.TEMPLATE_ID ->
                    receivedStatus.put(correlationId, codec.getSuccessMessage(buffer, bufferOffset, actingBlockLength, actingVersion));
            case ExecutionResultDecoder.TEMPLATE_ID -> receivedExecutionResults.put(correlationId,
                    codec.getExecutionResult(buffer, bufferOffset, actingBlockLength, actingVersion));
            case OrderIdDecoder.TEMPLATE_ID ->
                    receivedOrderId.put(correlationId, codec.getOrderId(buffer, bufferOffset, actingBlockLength, actingVersion));
            case EndOfOrdersDecoder.TEMPLATE_ID -> receivedOrders.put(correlationId, getOrdersResponse(correlationId));
            default -> throw new BadFieldException("method not supported");
        }
    }

    public void addOrderToCollectionOfAllOrders(final long correlationId, final DirectBuffer buffer, final int offset,
                                                final int actingBlockLength, final int actingVersion)
    {
        Order order = codec.getOrder(buffer, offset, actingBlockLength, actingVersion);
        List<Order> listToUpdate = ordersRequests.getOrDefault(correlationId, new ArrayList<>());
        listToUpdate.add(order);
        ordersRequests.put(correlationId, listToUpdate);
    }

    protected List<Order> getOrdersResponse(final long correlationId)
    {
        var removed = ordersRequests.remove(correlationId);
        if (removed == null)
        {
            return new ArrayList<>();
        }
        return removed;
    }

    public boolean receivedExecutionResult(final long currentCorrelationId)
    {
        return receivedExecutionResults.containsKey(currentCorrelationId);
    }

    public ExecutionResult getReceivedExecutionResult(final long correlationId)
    {
        return receivedExecutionResults.get(correlationId);
    }

    public boolean receivedStatus(final long currentCorrelationId)
    {
        return receivedStatus.containsKey(currentCorrelationId);
    }

    public Status getReceivedStatus(final long correlationId)
    {
        return receivedStatus.get(correlationId);
    }

    public boolean receivedOrderId(final long currentCorrelationId)
    {
        return receivedOrderId.containsKey(currentCorrelationId);
    }

    public long getReceivedOrderId(final long correlationId)
    {
        return receivedOrderId.get(correlationId);
    }

    public boolean receivedOrders(final long currentCorrelationId)
    {
        return receivedOrders.containsKey(currentCorrelationId);
    }

    public List<Order> getReceivedOrders(final long correlationId)
    {
        return receivedOrders.get(correlationId);
    }
}
