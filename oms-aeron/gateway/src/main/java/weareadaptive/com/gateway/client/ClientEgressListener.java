package weareadaptive.com.gateway.client;

import com.weareadaptive.sbe.*;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weareadaptive.com.gateway.exception.BadFieldException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientEgressListener implements EgressListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientEgressListener.class);
    private int currentLeader = -1;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final Map<Long, ServerWebSocket> allWebsockets = new ConcurrentHashMap<>();
    private final Decoder decoder = new Decoder();

    @Override
    public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                          final int offset, final int length,
                          final Header header)
    {
        LOGGER.info("Received message");
        int bufferOffset = offset;
        messageHeaderDecoder.wrap(buffer, bufferOffset);
        final int typeOfMessage = messageHeaderDecoder.templateId();
        final long correlationId = messageHeaderDecoder.correlationId();

        // todo: i think there's a stupid race condition here
        if (allWebsockets.containsKey(correlationId))
        {
            final int actingBlockLength = messageHeaderDecoder.blockLength();
            final int actingVersion = messageHeaderDecoder.version();
            bufferOffset += messageHeaderDecoder.encodedLength();

            if (typeOfMessage == OrderDecoder.TEMPLATE_ID) {
                decoder.addOrderToCollectionOfAllOrders(correlationId, buffer, bufferOffset, actingBlockLength, actingVersion);
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
            case SuccessMessageDecoder.TEMPLATE_ID -> decoder.getSuccessMessageAsJson(buffer, bufferOffset, actingBlockLength, actingVersion);
            case ExecutionResultDecoder.TEMPLATE_ID -> decoder.getExecutionResultAsJson(buffer, bufferOffset, actingBlockLength, actingVersion);
            case OrderIdDecoder.TEMPLATE_ID -> decoder.getOrderIdResponse(buffer, bufferOffset, actingBlockLength, actingVersion);
            case EndOfOrdersDecoder.TEMPLATE_ID -> decoder.getOrdersResponse(correlationId);
            default -> throw new BadFieldException("method not supported");
        };

        LOGGER.info("Sending message with correlation ID ".concat(String.valueOf(correlationId)));
        allWebsockets.get(correlationId).write(jsonObject.toBuffer());
        allWebsockets.remove(correlationId);
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
}
