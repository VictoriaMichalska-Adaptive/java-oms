package weareadaptive.com.gateway.ws;

import com.weareadaptive.sbe.MessageHeaderEncoder;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import weareadaptive.com.cluster.services.oms.util.Method;
import weareadaptive.com.cluster.services.oms.util.Side;
import weareadaptive.com.gateway.client.ClientEgressListener;
import weareadaptive.com.gateway.client.ClientIngressSender;
import weareadaptive.com.gateway.exception.BadFieldException;

public class WebSocketServer extends AbstractVerticle
{
    ClientIngressSender clientIngressSender;
    ClientEgressListener clientEgressListener;
    private long id = 0L;
    final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
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
        clientIngressSender.sendOrderRequestToCluster(correlationId, jsonEvent.getDouble("price"),
                jsonEvent.getLong("size"),
                Side.valueOf(jsonEvent.getString("side")));
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
        clientIngressSender.sendCancelOrderToCluster(correlationId, orderId);
    }

    private void WSHeaderMessage(final ServerWebSocket ws, final long correlationId, final Method method)
    {
        clientEgressListener.addWebsocket(correlationId, ws);
        clientIngressSender.sendHeaderMessageToCluster(correlationId, method);
    }
}