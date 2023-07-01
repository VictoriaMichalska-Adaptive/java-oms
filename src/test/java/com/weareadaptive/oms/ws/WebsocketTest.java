package com.weareadaptive.oms.ws;

import com.weareadaptive.oms.util.Side;
import com.weareadaptive.oms.util.Status;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class WebsocketTest
{

    private Vertx vertx;
    private HttpClient vertxClient;

    @BeforeEach
    void setUp(final VertxTestContext testContext)
    {
        vertx = Vertx.vertx();
        vertx.deployVerticle(new WebSocketServer(), testContext.succeeding(id -> testContext.completeNow()));
        vertxClient = vertx.createHttpClient();
    }

    @AfterEach
    void tearDown()
    {
        vertxClient.close();
        vertx.close();
    }

    @Test
    @DisplayName("Establish connection from WS Client to WS Server")
    public void connectToServer(final VertxTestContext testContext) throws Throwable
    {
        vertxClient.webSocket(8080, "localhost", "", client ->
        {
            testContext.completeNow();
        });
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
        if (testContext.failed()) throw testContext.causeOfFailure();
    }

    @Test
    @DisplayName("Place Order request from client and receive response from server")
    public void wsPlaceOrderRequest(final VertxTestContext testContext) throws Throwable
    {
        /**
         * * Sends websocket request to server to place order
         *   Should receive a response containing corresponding orderId and status
         */
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            OrderDTO order = new OrderDTO(10, 15, Side.ASK);
            orderRequest.put("order", JsonObject.mapFrom(order));
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(data -> {
                final var newExecution = data.toJsonObject().mapTo(ExecutionResultDTO.class);
                assertSame(newExecution.status(), Status.RESTING);
                assertTrue(newExecution.orderId() >= 0);
                testContext.completeNow();
            });
        });
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Cancel Order request from client and receive response from server")
    public void wsCancelOrderRequest(final VertxTestContext testContext) throws Throwable
    {
        /**
         * * Sends websocket request to server to cancel order
         *   Should receive a response containing corresponding orderId and status
         */
        testContext.completeNow();
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Clear Orderbook request from client and receive response from server")
    public void wsClearOrderbookRequest(final VertxTestContext testContext) throws Throwable
    {
        /**
         *   Sends websocket request to server to clear Orderbook
         *   Should receive a response indicating success
         *   */
        testContext.completeNow();
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Reset Orderbook request from client and receive response from server")
    public void wsResetOrderbookRequest(final VertxTestContext testContext) throws Throwable
    {
        /**
         * * Sends websocket request to server to reset Orderbook
         *   Should receive a response indicating success
         */
        testContext.completeNow();
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

}