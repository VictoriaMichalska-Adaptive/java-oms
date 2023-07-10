package com.weareadaptive.gateway.ws;

import com.weareadaptive.gateway.client.ClientEgressListener;
import com.weareadaptive.gateway.client.ClientIngressSender;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.ServerWebSocket;

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
    }

    /**
     * * Handle request into Cluster, return ExecutionResult response to client
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
    private void WSPlaceOrder(final ServerWebSocket ws)
    {
    }

    /**
     * * Handle request into Cluster, return ExecutionResult response to client
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
    private void WSCancelOrder(final ServerWebSocket ws)
    {
    }

    /**
     * * Handle request into Cluster, return status response to client
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
    }

    /**
     * * Handle request into Cluster, return status response to client
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
    }
}
