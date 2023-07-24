package com.weareadaptive.gateway.client;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Method;
import com.weareadaptive.cluster.services.oms.util.Status;
import com.weareadaptive.gateway.exception.BadFieldException;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.weareadaptive.util.Decoder.*;

public class ClientEgressListener implements EgressListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientEgressListener.class);
    private int currentLeader = -1;
    private final BiMap<Long, ServerWebSocket> allWebsockets = HashBiMap.create();
    private final Map<Long, Method> allMethods = new HashMap<>();

    @Override
    public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                          final int offset, final int length,
                          final Header header)
    {
        LOGGER.info("Message has been received");
        long id = decodeLongId(buffer, offset);
        if (allWebsockets.containsKey(id)) {

            final var responseType = allMethods.get(id);
            JsonObject sendThisMessage = getJsonObjectFromMessage(responseType, buffer, offset);

            allWebsockets.get(id).write(sendThisMessage.toBuffer());
            allWebsockets.remove(id);
            allMethods.remove(id);
        }
        else {
            LOGGER.error("This ID is not in use: ".concat(String.valueOf(id)));
        }
    }

    private JsonObject getJsonObjectFromMessage(Method method, DirectBuffer buffer, int offset)
    {
        return switch (method)
                {
                    case CLEAR, RESET -> getSuccessMessageAsJson(buffer, offset + ID_SIZE);
                    case PLACE, CANCEL -> getExecutionResultAsJson(buffer, offset + ID_SIZE);
                    default -> throw new BadFieldException("method not supported");
                };
    }

    private JsonObject getExecutionResultAsJson(DirectBuffer buffer, int offset)
    {
        final ExecutionResult executionResult = decodeExecutionResult(buffer, offset);
        LOGGER.info("Sending back ExecutionResult for OrderId %d with status %s".formatted(executionResult.getOrderId(), executionResult.getStatus().name()));
        return JsonObject.mapFrom(executionResult);
    }

    private JsonObject getSuccessMessageAsJson(DirectBuffer buffer, int offset)
    {
        final Status status = decodeStatus(buffer, offset);
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

    public void addWebsocket(long id, ServerWebSocket ws, Method method) {
        allWebsockets.put(id, ws);
        allMethods.put(id, method);
    }

}
