package com.weareadaptive.oms.ws;

import com.weareadaptive.oms.OrderbookImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;

public class WebSocketServer extends AbstractVerticle
{
    OrderbookImpl orderbook;

    @Override
    public void start()
    {
        this.orderbook = new OrderbookImpl();
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
        System.out.println("ask for help");
        ws.handler(event -> {
            var jsonEvent = event.toJsonObject();
            if (jsonEvent.containsKey("method")) {
                final var eventMethod = jsonEvent.getString("method");
                switch (eventMethod)
                {
                    case "place" -> WSPlaceOrder(ws, jsonEvent);
                    case "cancel" -> WSCancelOrder(ws, jsonEvent);
                    case "clear" -> WSClearOrderbook(ws, jsonEvent);
                    case "reset" -> WSResetOrderbook(ws, jsonEvent);
                    default -> throw new IllegalArgumentException("Method does not exist: ".concat(eventMethod));
                }
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
        System.out.println("miss, id like to place an order");
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
    private void WSCancelOrder(final ServerWebSocket ws, JsonObject jsonEvent)
    {
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
    private void WSClearOrderbook(final ServerWebSocket ws, JsonObject jsonEvent)
    {
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
    private void WSResetOrderbook(final ServerWebSocket ws, JsonObject jsonEvent)
    {
    }
}