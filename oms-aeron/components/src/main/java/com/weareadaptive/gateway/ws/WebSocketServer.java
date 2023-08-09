package com.weareadaptive.gateway.ws;

import com.weareadaptive.cluster.services.oms.util.Method;
import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.gateway.client.ClientEgressListener;
import com.weareadaptive.gateway.client.ClientIngressSender;
import com.weareadaptive.gateway.exception.BadFieldException;
import com.weareadaptive.sbe.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class WebSocketServer extends AbstractVerticle
{
    ClientIngressSender clientIngressSender;
    ClientEgressListener clientEgressListener;
    private long id = 0L;
    final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    final OrderRequestEncoder orderRequestEncoder = new OrderRequestEncoder();
    final CancelRequestEncoder cancelOrderEncoder = new CancelRequestEncoder();
    final private AsksRequestEncoder asksRequestEncoder = new AsksRequestEncoder();
    final private BidsRequestEncoder bidsRequestEncoder = new BidsRequestEncoder();
    final private ResetRequestEncoder resetRequestEncoder = new ResetRequestEncoder();
    final private ClearRequestEncoder clearRequestEncoder = new ClearRequestEncoder();
    final private CurrentIdRequestEncoder currentIdRequestEncoder = new CurrentIdRequestEncoder();

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
                    case "clear" -> WSHeaderMessage(ws, ++id, Method.CLEAR);
                    case "reset" -> WSHeaderMessage(ws, ++id, Method.RESET);
                    case "bids" -> WSHeaderMessage(ws, ++id, Method.BIDS);
                    case "asks" -> WSHeaderMessage(ws, ++id, Method.ASKS);
                    case "orderId" -> WSHeaderMessage(ws, ++id, Method.CURRENT_ORDER_ID);
                    default -> throw new BadFieldException("method");
                }
            }
            catch (BadFieldException | ClassCastException exception)
            {
                ws.write(JsonObject.of("code", "400").toBuffer());
            }
        });
    }

    private void setHeaderEncoder(final MutableDirectBuffer buffer, final long correlationId) {
        headerEncoder.wrap(buffer, 0);
        headerEncoder.correlationId(correlationId);
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
    private void WSPlaceOrder(final ServerWebSocket ws, long correlationId, JsonObject jsonEvent)
    {
        if (jsonEvent == null) { throw new BadFieldException("order"); }
        if (jsonEvent.getDouble("price") == null) { throw new BadFieldException("price"); }
        if (jsonEvent.getLong("size") == null) { throw new BadFieldException("size"); }
        if (jsonEvent.getString("side") == null) { throw new BadFieldException("side"); }

        clientEgressListener.addWebsocket(correlationId, ws);

        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + OrderRequestEncoder.BLOCK_LENGTH;
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(encodedLength);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);
        orderRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);

        orderRequestEncoder.price(jsonEvent.getDouble("price"));
        orderRequestEncoder.size(jsonEvent.getLong("size"));
        orderRequestEncoder.side(Side.valueOf(jsonEvent.getString("side")).getByte());

        clientIngressSender.sendMessageToCluster(directBuffer, encodedLength);
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
    private void WSCancelOrder(final ServerWebSocket ws, long correlationId, long orderId)
    {
        clientEgressListener.addWebsocket(correlationId, ws);

        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + CancelRequestEncoder.BLOCK_LENGTH;
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(encodedLength);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);
        cancelOrderEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
        cancelOrderEncoder.orderId(orderId);

        clientIngressSender.sendMessageToCluster(directBuffer, encodedLength);
    }

    private void WSHeaderMessage(final ServerWebSocket ws, final long correlationId, final Method method)
    {
        clientEgressListener.addWebsocket(correlationId, ws);
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH;
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(encodedLength);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);

        switch (method) {
            case CLEAR -> clearRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case RESET -> resetRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case BIDS -> bidsRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case ASKS -> asksRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case CURRENT_ORDER_ID -> currentIdRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
        }
        clientIngressSender.sendMessageToCluster(directBuffer, encodedLength);
    }
}