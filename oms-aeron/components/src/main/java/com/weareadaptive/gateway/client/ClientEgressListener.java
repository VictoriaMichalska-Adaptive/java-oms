package com.weareadaptive.gateway.client;

import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Order;
import com.weareadaptive.cluster.services.oms.util.Status;
import com.weareadaptive.gateway.exception.BadFieldException;
import com.weareadaptive.sbe.*;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientEgressListener implements EgressListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientEgressListener.class);
    private int currentLeader = -1;
    private final Map<Long, ServerWebSocket> allWebsockets = new ConcurrentHashMap<>();
    private final Map<Long, List<Order>> ordersRequests = new ConcurrentHashMap<>();
    final ExecutionResult executionResult = new ExecutionResult();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final OrderIdDecoder orderIdDecoder = new OrderIdDecoder();
    private final ExecutionResultDecoder executionResultDecoder = new ExecutionResultDecoder();
    private final SuccessMessageDecoder successMessageDecoder = new SuccessMessageDecoder();
    private final OrderDecoder orderDecoder = new OrderDecoder();

    @Override
    public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                          final int offset, final int length,
                          final Header header)
    {
        LOGGER.info("Message has been received");

        int bufferOffset = offset;
        messageHeaderDecoder.wrap(buffer, bufferOffset);
        final int typeOfMessage = messageHeaderDecoder.templateId();
        final long correlationId = messageHeaderDecoder.correlationId();

        if (allWebsockets.containsKey(correlationId))
        {
            final int actingBlockLength = messageHeaderDecoder.blockLength();
            final int actingVersion = messageHeaderDecoder.version();
            bufferOffset += messageHeaderDecoder.encodedLength();

            if (typeOfMessage == OrderDecoder.TEMPLATE_ID) {
                addOrderToCollectionOfAllOrders(correlationId, buffer, bufferOffset, actingBlockLength, actingVersion);
            }
            else
            {
                sendMessage(buffer, bufferOffset, typeOfMessage, correlationId, actingBlockLength, actingVersion);
            }
        }
        else {
            LOGGER.error("This ID is not in use: ".concat(String.valueOf(correlationId)));
        }
    }

    private void sendMessage(final DirectBuffer buffer, final int bufferOffset, final int typeOfMessage, final long correlationId,
                             final int actingBlockLength, final int actingVersion)
    {
        JsonObject jsonObject = switch (typeOfMessage)
        {
            case SuccessMessageDecoder.TEMPLATE_ID -> getSuccessMessageAsJson(buffer, bufferOffset, actingBlockLength, actingVersion);
            case ExecutionResultDecoder.TEMPLATE_ID -> getExecutionResultAsJson(buffer, bufferOffset, actingBlockLength, actingVersion);
            case OrderIdDecoder.TEMPLATE_ID -> getOrderIdResponse(buffer, bufferOffset, actingBlockLength, actingVersion);
            case EndOfOrdersDecoder.TEMPLATE_ID -> getOrdersResponse(correlationId);
            default -> throw new BadFieldException("method not supported");
        };

        allWebsockets.get(correlationId).write(jsonObject.toBuffer());
        allWebsockets.remove(correlationId);
    }

    private JsonObject getOrdersResponse(final long correlationId)
    {
        List<Order> orders = ordersRequests.get(correlationId);
        JsonArray jsonArray = new JsonArray();
        for (Order order : orders) {
            JsonObject orderJson = JsonObject.mapFrom(order);
            jsonArray.add(orderJson);
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("orders", jsonArray);
        LOGGER.info(jsonObject.toString());
        return jsonObject;
    }

    private JsonObject getOrderIdResponse(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        orderIdDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        return JsonObject.of("orderId", orderIdDecoder.orderId());
    }

    private JsonObject getExecutionResultAsJson(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        executionResultDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final long orderId = executionResultDecoder.orderId();
        final Status status = Status.fromByteValue((byte) executionResultDecoder.status());
        executionResult.setStatus(status);
        executionResult.setOrderId(orderId);
        LOGGER.info("Sending back ExecutionResult for OrderId %d with status %s".formatted(executionResult.getOrderId(), executionResult.getStatus().name()));
        return JsonObject.mapFrom(executionResult);
    }

    private JsonObject getSuccessMessageAsJson(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        successMessageDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final Status status = Status.fromByteValue((byte) successMessageDecoder.status());
        LOGGER.info("Sending back status %s".formatted(status.name()));
        return JsonObject.of("status", status.name());
    }

    @Override
    public void onSessionEvent(final long correlationId, final long clusterSessionId, final long leadershipTermId,
                               final int leaderMemberId,
                               final EventCode code, final String detail)
    {
        EgressListener.super.onSessionEvent(correlationId, clusterSessionId, leadershipTermId, leaderMemberId, code,
                detail);
    }

    @Override
    public void onNewLeader(final long clusterSessionId, final long leadershipTermId, final int leaderMemberId,
                            final String ingressEndpoints)
    {
        LOGGER.info("Cluster node " + leaderMemberId + " is now Leader, previous Leader: " + currentLeader);
        currentLeader = leaderMemberId;
    }

    public int getCurrentLeader()
    {
        return this.currentLeader;
    }

    public void addWebsocket(final long id, final ServerWebSocket ws) {
        allWebsockets.put(id, ws);
    }

    public void addOrderToCollectionOfAllOrders(final long correlationId, final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion) {
        orderDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final Order order = new Order(orderDecoder.orderId(), orderDecoder.price(), orderDecoder.size());
        List<Order> listToUpdate = ordersRequests.getOrDefault(correlationId, new ArrayList<>());
        listToUpdate.add(order);
        LOGGER.info(order.toString());
        ordersRequests.put(correlationId, listToUpdate);
    }
}
