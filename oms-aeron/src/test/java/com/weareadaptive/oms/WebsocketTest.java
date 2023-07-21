package com.weareadaptive.oms;

import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.cluster.services.oms.util.Status;
import com.weareadaptive.gateway.ws.command.ErrorCommand;
import com.weareadaptive.gateway.ws.command.ExecutionResultCommand;
import com.weareadaptive.gateway.ws.command.OrderCommand;
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

import static io.vertx.core.Vertx.vertx;
import static org.junit.jupiter.api.Assertions.*;

// todo: additional test cases
// todo: stop using this hacky version of testing for vertx?
@ExtendWith(VertxExtension.class)
public class WebsocketTest
{
    private HttpClient vertxClient;

    private Deployment deployment;

    @BeforeEach
    void setUp(final VertxTestContext testContext) throws InterruptedException
    {
        deployment = new Deployment();
        deployment.startCluster();
        deployment.startGateway(testContext.succeeding(id -> testContext.completeNow()));
        vertxClient = vertx().createHttpClient();
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        vertxClient.close();
        deployment.shutdownCluster();
        deployment.shutdownGateway();
    }

    @Test
    @DisplayName("Establish connection from WS Client to WS Server")
    public void connectToServer(final VertxTestContext testContext) throws Throwable
    {
        vertxClient.webSocket(8080, "localhost", "/", client -> {
            testContext.completeNow();
        });
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
        if (testContext.failed()) throw testContext.causeOfFailure();
    }

    @Test
    @DisplayName("Place Order request from client and receive response from server")
    public void wsPlaceOrderRequest(final VertxTestContext testContext) throws Throwable
    {
        /*
         *   Sends websocket request to server to place order
         *   Should receive a response containing corresponding orderId and status
         */
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            OrderCommand order = new OrderCommand(10, 15, Side.ASK);
            orderRequest.put("order", JsonObject.mapFrom(order));
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(data -> {
                final var newExecution = data.toJsonObject().mapTo(ExecutionResultCommand.class);

                assertSame(Status.RESTING, newExecution.status());
                assertTrue(newExecution.orderId() >= 0);
                testContext.completeNow();
            });
        });
        assertTrue(testContext.awaitCompletion(10, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Give fake method request from client and receive error response from server")
    public void wsFakeMethodRequest(final VertxTestContext testContext) throws Throwable
    {
        /*
         *   Sends websocket request to server to place order
         *   Should receive a response containing corresponding orderId and status
         */
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject fakeRequest = new JsonObject();
            fakeRequest.put("method", "fakeMethod");
            websocket.write(fakeRequest.toBuffer());

            websocket.handler(data -> {
                final var errorDTO = data.toJsonObject().mapTo(ErrorCommand.class);
                assertEquals( 400, errorDTO.code());
                testContext.completeNow();
            });
        });
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Place Order request without an Order from client and receive response from server")
    public void wsFailedPlaceOrderRequest(final VertxTestContext testContext) throws Throwable
    {
        /*
         *   Sends websocket request to server to place order
         *   Should receive a response containing corresponding orderId and status
         */
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(data -> {
                final var error = data.toJsonObject().mapTo(ErrorCommand.class);
                assertEquals(400, error.code());
                testContext.completeNow();
            });
        });
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Give fake method request from client and receive error response from server")
    public void wsBadOrderRequest(final VertxTestContext testContext) throws Throwable
    {
        /*
         *   Sends websocket request to server to place order
         *   Should receive a response containing corresponding orderId and status
         */
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            orderRequest.put("order", "nonsense");
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(data -> {
                final var errorDTO = data.toJsonObject().mapTo(ErrorCommand.class);
                assertEquals( 400, errorDTO.code());
                testContext.completeNow();
            });
        });
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Cancel Order request from client and receive response from server")
    public void wsCancelOrderRequest(final VertxTestContext testContext) throws Throwable
    {
        /*
         *   Sends websocket request to server to cancel order
         *   Should receive a response containing corresponding orderId and status
         */
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            OrderCommand order = new OrderCommand(10, 15, Side.ASK);
            orderRequest.put("order", JsonObject.mapFrom(order));
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(placeOrderResponse -> {
                long orderId = placeOrderResponse.toJsonObject().getLong("orderId");

                JsonObject cancelRequest = new JsonObject();
                cancelRequest.put("method", "cancel");
                cancelRequest.put("orderId", orderId);
                Buffer cancel = Buffer.buffer(cancelRequest.encode());
                websocket.write(cancel);

                websocket.handler(cancelOrderResponse -> {
                    final var executionResult = cancelOrderResponse.toJsonObject().mapTo(ExecutionResultCommand.class);
                    if (executionResult.status() == Status.CANCELLED && executionResult.orderId() == orderId) {
                        testContext.completeNow();
                    }
                });
            });

        });
        assertTrue(testContext.awaitCompletion(10, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Clear Orderbook request from client and receive response from server")
    public void wsClearOrderbookRequest(final VertxTestContext testContext) throws Throwable
    {
        /*
         *   Sends websocket request to server to clear Orderbook
         *   Should receive a response indicating success
         */
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "clear");
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(data -> {
                assertEquals("SUCCESS", data.toJsonObject().getString("status"));
                testContext.completeNow();
            });
        });
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Reset Orderbook request from client and receive response from server")
    public void wsResetOrderbookRequest(final VertxTestContext testContext) throws Throwable
    {
        /*
         *   Sends websocket request to server to reset Orderbook
         *   Should receive a response indicating success
         */
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "clear");
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(data -> {
                assertEquals("SUCCESS", data.toJsonObject().getString("status"));
                testContext.completeNow();
            });
        });
        assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
    }

}