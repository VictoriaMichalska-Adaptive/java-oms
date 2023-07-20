package com.weareadaptive.gateway.ws;

import com.weareadaptive.cluster.services.util.ServiceName;
import com.weareadaptive.cluster.services.oms.util.Method;
import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.gateway.client.ClientEgressListener;
import com.weareadaptive.gateway.client.ClientIngressSender;
import com.weareadaptive.gateway.exception.BadFieldException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static com.weareadaptive.util.Decoder.*;

public class WebSocketServer extends AbstractVerticle
{
    ClientIngressSender clientIngressSender;
    ClientEgressListener clientEgressListener;
    private long id = 0L;

    public WebSocketServer(final ClientIngressSender clientIngressSender,
                           final ClientEgressListener clientEgressListener)
    {
        this.clientIngressSender = clientIngressSender;
        this.clientEgressListener = clientEgressListener;
    }

    @Override
    public void start()
    {
        vertx.createHttpServer()
                .webSocketHandler(this::WSHandler)
                .listen(8080);
    }

    /**
     * * Handle incoming websocket requests and implement routing logic
     * - Routing to appropriate logic should be handled via JSON payload:
     * - e.g: JSON payload to route to place order
     * {
     * "method": "place"
     * "order":
     * {
     * "price": 10.00
     * "size": 15
     * "side": "BID"
     * }
     * }
     */
    private void WSHandler(final ServerWebSocket ws)
    {
        ws.handler(event -> {
            var jsonEvent = event.toJsonObject();
            try
            {
                final var eventMethod = jsonEvent.getString("method");
                switch (eventMethod)
                {
                    case "place" -> WSPlaceOrder(ws, ++id, jsonEvent.getJsonObject("order"));
                    case "cancel" -> WSCancelOrder(ws, ++id, jsonEvent.getLong("orderId"));
                    case "clear" -> WSClearOrderbook(ws, ++id);
                    case "reset" -> WSResetOrderbook(ws, ++id);
                    default -> throw new BadFieldException("method");
                }
            }
            catch (BadFieldException | ClassCastException exception)
            {
                ws.write(JsonObject.of("code", "400").toBuffer());
            }
        });
    }

    /**
     * * Handle request into Orderbook, return ExecutionResult response to client
     * <p>
     * - e.g: JSON payload request
     * {
     * "method": "place"
     * "order":
     * {
     * "price": 10.00
     * "size": 15
     * "side": "BID"
     * }
     * }
     * <p>
     * - e.g: JSON response
     * {
     * "orderId": 1
     * "status": "FILLED"
     * }
     */
    private void WSPlaceOrder(final ServerWebSocket ws, long messageId, JsonObject jsonEvent)
    {
        if (jsonEvent == null) { throw new BadFieldException("order"); }
        if (jsonEvent.getDouble("price") == null) { throw new BadFieldException("price"); }
        if (jsonEvent.getLong("size") == null) { throw new BadFieldException("size"); }
        if (jsonEvent.getString("side") == null) { throw new BadFieldException("side"); }

        clientEgressListener.addWebsocket(messageId, ws, Method.PLACE);

        MutableDirectBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_SIZE + ORDER_SIZE));
        buffer.putBytes(0, encodeOMSHeader(ServiceName.OMS, Method.PLACE, id), 0, HEADER_SIZE);
        buffer.putBytes(HEADER_SIZE, encodeOrderRequest(jsonEvent.getDouble("price"),
                jsonEvent.getLong("size"), Side.valueOf(jsonEvent.getString("side"))), 0, ORDER_SIZE);

        clientIngressSender.sendMessageToCluster(buffer, HEADER_SIZE + ORDER_SIZE);
    }

    /**
     * * Handle request into Orderbook, return ExecutionResult response to client
     * <p>
     * - e.g: JSON payload request
     * {
     * "method": "cancel"
     * "orderId" : 1
     * }
     * <p>
     * - e.g: JSON response
     * {
     * "orderId": 1
     * "status": "CANCELLED"
     * }
     */
    private void WSCancelOrder(final ServerWebSocket ws, Long messageId, Long orderId)
    {
        if (orderId == null) throw new BadFieldException("orderId");
        System.out.println("messageId " + messageId);
        System.out.println("orderId " + orderId);

        clientEgressListener.addWebsocket(messageId, ws, Method.CANCEL);
        MutableDirectBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_SIZE + ID_SIZE));
        buffer.putBytes(0, encodeOMSHeader(ServiceName.OMS, Method.CANCEL, messageId), 0, HEADER_SIZE);
        buffer.putBytes(HEADER_SIZE, encodeId(orderId), 0, ID_SIZE);

        clientIngressSender.sendMessageToCluster(buffer, HEADER_SIZE + ID_SIZE);
    }

    /**
     * * Handle request into Orderbook, return status response to client
     * <p>
     * - e.g: JSON payload request
     * {
     * "method": "clear"
     * }
     * <p>
     * - e.g: JSON response
     * {
     * "status": "SUCCESS"
     * }
     */
    private void WSClearOrderbook(final ServerWebSocket ws, final Long messageId)
    {
        clientEgressListener.addWebsocket(messageId, ws, Method.CLEAR);
        MutableDirectBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_SIZE));
        buffer.putBytes(0, encodeOMSHeader(ServiceName.OMS, Method.CLEAR, id), 0, HEADER_SIZE);

        clientIngressSender.sendMessageToCluster(buffer, HEADER_SIZE);
    }

    /**
     * * Handle request into Orderbook, return status response to client
     * <p>
     * - e.g: JSON payload request
     * {
     * "method": "reset"
     * }
     * <p>
     * - e.g: JSON response
     * {
     * "status": "SUCCESS"
     * }
     */
    private void WSResetOrderbook(final ServerWebSocket ws, final Long messageId)
    {
        clientEgressListener.addWebsocket(messageId, ws, Method.RESET);

        MutableDirectBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_SIZE));
        buffer.putBytes(0, encodeOMSHeader(ServiceName.OMS, Method.RESET, id), 0, HEADER_SIZE);

        clientIngressSender.sendMessageToCluster(buffer, HEADER_SIZE);
    }
}