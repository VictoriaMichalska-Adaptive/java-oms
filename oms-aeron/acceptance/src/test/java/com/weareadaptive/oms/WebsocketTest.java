package com.weareadaptive.oms;

import com.weareadaptive.oms.util.TestOrder;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import weareadaptive.com.cluster.services.oms.util.Side;
import weareadaptive.com.cluster.services.oms.util.Status;
import weareadaptive.com.gateway.ws.command.ErrorCommand;
import weareadaptive.com.gateway.ws.command.ExecutionResultCommand;
import weareadaptive.com.gateway.ws.command.OrderCommand;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.vertx.core.Vertx.vertx;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class WebsocketTest
{
    private HttpClient vertxClient;

    private Deployment deployment;

    @BeforeEach
    void setUp(final VertxTestContext testContext) throws InterruptedException
    {
        deployment = new Deployment();
        deployment.startCluster(3, true);
        deployment.startGateway(3, testContext.succeeding(id -> testContext.completeNow()));
        vertxClient = vertx().createHttpClient();
    }

    @AfterEach
    void tearDown()
    {
        vertxClient.close();
        deployment.shutdownGateway();
        deployment.shutdownCluster();
    }

    @Test
    @DisplayName("Establish connection from WS Client to WS Server")
    public void connectToServer(final VertxTestContext testContext) throws Throwable
    {
        vertxClient.webSocket(8080, "localhost", "/", client -> testContext.completeNow());
        assertTrue(testContext.awaitCompletion(10, TimeUnit.SECONDS));
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
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("GetCurrentId from client and receive response from server")
    public void wsGetCurrentIdRequest(final VertxTestContext testContext) throws Throwable
    {
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            OrderCommand order = new OrderCommand(10, 15, Side.ASK);
            orderRequest.put("order", JsonObject.mapFrom(order));
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(placeOrderResponse -> {
                long orderId = placeOrderResponse.toJsonObject().getLong("orderId");

                JsonObject orderIdRequest = new JsonObject();
                orderIdRequest.put("method", "orderId");
                Buffer getCurrentId = Buffer.buffer(orderIdRequest.encode());
                websocket.write(getCurrentId);

                websocket.handler(response -> {
                    final var currentId = response.toJsonObject().getLong("orderId");
                    assertEquals(orderId + 1, currentId);
                    testContext.completeNow();
                });
            });

        });
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("GetAsks request from client and receive response from server")
    public void wsGetAsksRequest(final VertxTestContext testContext) throws Throwable
    {
        List<TestOrder> expectedOrders = new ArrayList<>();
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            OrderCommand order = new OrderCommand(10, 15, Side.ASK);
            orderRequest.put("order", JsonObject.mapFrom(order));
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);
            websocket.write(request);

            websocket.handler(placeOrderResponse -> {
                long orderId = placeOrderResponse.toJsonObject().getLong("orderId");
                expectedOrders.add(new TestOrder(orderId, 10, 15));

                websocket.handler(secondPlaceOrderResponse -> {
                    long secondOrderId = secondPlaceOrderResponse.toJsonObject().getLong("orderId");
                    expectedOrders.add(new TestOrder(secondOrderId, 10, 15));

                    JsonObject asksRequest = new JsonObject();
                    asksRequest.put("method", "asks");
                    Buffer encodedRequest = Buffer.buffer(asksRequest.encode());
                    websocket.write(encodedRequest);

                    websocket.handler(response -> {
                        JsonArray jsonArray = response.toJsonObject().getJsonArray("orders");

                        List<TestOrder> orders = new LinkedList<>();
                        for (Object obj : jsonArray) {
                            if (obj instanceof JsonObject jsonObject) {
                                TestOrder thisOrder = jsonObject.mapTo(TestOrder.class);
                                orders.add(thisOrder);
                            }
                        }

                        assertEquals(expectedOrders, orders);
                        testContext.completeNow();
                    });
                });
            });

        });
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("GetBids request from client and receive response from server")
    public void wsGetBidsRequest(final VertxTestContext testContext) throws Throwable
    {
        List<TestOrder> expectedOrders = new ArrayList<>();
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            OrderCommand order = new OrderCommand(10, 15, Side.BID);
            orderRequest.put("order", JsonObject.mapFrom(order));
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);
            websocket.write(request);

            websocket.handler(placeOrderResponse -> {
                long orderId = placeOrderResponse.toJsonObject().getLong("orderId");
                expectedOrders.add(new TestOrder(orderId, 10, 15));

                websocket.handler(secondPlaceOrderResponse -> {
                    long secondOrderId = secondPlaceOrderResponse.toJsonObject().getLong("orderId");
                    expectedOrders.add(new TestOrder(secondOrderId, 10, 15));

                    JsonObject asksRequest = new JsonObject();
                    asksRequest.put("method", "bids");
                    Buffer encodedRequest = Buffer.buffer(asksRequest.encode());
                    websocket.write(encodedRequest);

                    websocket.handler(response -> {
                        JsonArray jsonArray = response.toJsonObject().getJsonArray("orders");

                        List<TestOrder> orders = new LinkedList<>();
                        for (Object obj : jsonArray) {
                            if (obj instanceof JsonObject jsonObject) {
                                TestOrder thisOrder = jsonObject.mapTo(TestOrder.class);
                                orders.add(thisOrder);
                            }
                        }

                        assertEquals(expectedOrders, orders);
                        testContext.completeNow();
                    });
                });
            });

        });
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
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
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
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
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
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
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
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
                    assertEquals(Status.CANCELLED, executionResult.status());
                    assertEquals(orderId, executionResult.orderId());
                    testContext.completeNow();
                });
            });

        });
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
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
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
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
            orderRequest.put("method", "reset");
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(data -> {
                assertEquals("SUCCESS", data.toJsonObject().getString("status"));
                testContext.completeNow();
            });
        });
        assertTrue(testContext.awaitCompletion(20, TimeUnit.SECONDS));
    }

}