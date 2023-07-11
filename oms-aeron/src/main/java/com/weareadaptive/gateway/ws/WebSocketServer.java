package com.weareadaptive.gateway.ws;

import com.weareadaptive.gateway.client.ClientEgressListener;
import com.weareadaptive.gateway.client.ClientIngressSender;
import com.weareadaptive.gateway.ws.dto.OrderDTO;
import com.weareadaptive.gateway.ws.exception.MissingFieldException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;

public class WebSocketServer extends AbstractVerticle
{
    ClientIngressSender clientIngressSender;
    ClientEgressListener clientEgressListener;

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
                    case "place" -> WSPlaceOrder(ws, jsonEvent.getJsonObject("order"));
                    case "cancel" -> WSCancelOrder(ws, jsonEvent.getLong("orderId"));
                    case "clear" -> WSClearOrderbook(ws);
                    case "reset" -> WSResetOrderbook(ws);
                    default -> throw new MissingFieldException("method");
                }
            }
            catch (MissingFieldException | ClassCastException exception)
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
    private void WSPlaceOrder(final ServerWebSocket ws, JsonObject jsonEvent)
    {
        try
        {
            final var order = jsonEvent.mapTo(OrderDTO.class);
            // todo: replace with aeron call
            // final var executionResult = orderbook.placeOrder(order.price(), order.size(), order.side());
            // ws.write(JsonObject.mapFrom(executionResult).toBuffer());
        }
        catch (NullPointerException e) {
            throw new MissingFieldException("order");
        }
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
    private void WSCancelOrder(final ServerWebSocket ws, Long orderId)
    {
        if (orderId == null) throw new MissingFieldException("orderId");
        // todo: replace with aeron call
        // final var executionResult = orderbook.cancelOrder(orderId);
        // ws.write(JsonObject.mapFrom(executionResult).toBuffer());
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
    private void WSClearOrderbook(final ServerWebSocket ws)
    {
        // todo: replace with aeron call
        //orderbook.clear();
        ws.write(JsonObject.of("status", "SUCCESS").toBuffer());
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
    private void WSResetOrderbook(final ServerWebSocket ws)
    {
        // todo: replace with aeron call
        // orderbook.reset();
        ws.write(JsonObject.of("status", "SUCCESS").toBuffer());
    }
}