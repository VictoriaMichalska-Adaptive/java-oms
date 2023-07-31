package com.weareadaptive.oms;

import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.gateway.ws.command.OrderCommand;
import com.weareadaptive.oms.util.TestOrder;
import io.aeron.cluster.ClusterTool;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.core.Vertx.vertx;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
public class SnapshotTest
{
    private Deployment deployment;
    private HttpClient vertxClient;

    @BeforeEach
    void setUp(final VertxTestContext testContext) throws InterruptedException
    {
        deployment = new Deployment();
        deployment.startSingleNodeCluster(true);
        deployment.startGateway(testContext.succeeding(id -> testContext.completeNow()));
        vertxClient = vertx().createHttpClient();
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        vertxClient.close();
        deployment.shutdownGateway();
        deployment.shutdownCluster();
    }

    @Test
    public void clusterCanRecoverToStateFromSnapshot(final VertxTestContext testContext) throws Throwable
    {
        CountDownLatch shutdownLatch = new CountDownLatch(2);
        AtomicReference<List<TestOrder>> asks = new AtomicReference<>(null);
        AtomicReference<List<TestOrder>> bids = new AtomicReference<>(null);

        // set up non-default state
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            OrderCommand bidOrder = new OrderCommand(1, 100, Side.BID);
            bids.set(makeTwoOrdersAndCheckThatTheyOccurred(bidOrder, shutdownLatch));
        });

        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            OrderCommand askOrder = new OrderCommand(100,1, Side.ASK);
            asks.set(makeTwoOrdersAndCheckThatTheyOccurred(askOrder, shutdownLatch));
        });

        shutdownLatch.await();

        // snapshot, shut down, and reboot
        deployment.getNodes().forEach((id, node) -> ClusterTool.snapshot(node.getClusterDir(), System.out));

        deployment.shutdownCluster();
        deployment.shutdownGateway();
        deployment.startGateway();
        deployment.startSingleNodeCluster(false);

        // assert same state as original
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(webSocket -> getOrders(Side.ASK, webSocket, asks.get(), null));
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(webSocket -> getOrders(Side.BID, webSocket, bids.get(), testContext));

        assertTrue(testContext.awaitCompletion(10, TimeUnit.SECONDS));
    }

    private List<TestOrder> makeTwoOrdersAndCheckThatTheyOccurred(OrderCommand orderCommand, CountDownLatch bootupLatch)
    {
        List<TestOrder> expectedOrders = new ArrayList<>();
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            orderRequest.put("order", JsonObject.mapFrom(orderCommand));
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);

            websocket.handler(placeOrderResponse -> {
                long orderId = placeOrderResponse.toJsonObject().getLong("orderId");
                expectedOrders.add(new TestOrder(orderId, orderCommand.price(), orderCommand.size()));
                websocket.write(request);

                websocket.handler(secondPlaceOrderResponse -> {
                    long secondOrderId = secondPlaceOrderResponse.toJsonObject().getLong("orderId");
                    expectedOrders.add(new TestOrder(secondOrderId, orderCommand.price(), orderCommand.size()));

                    getOrders(orderCommand.side(), websocket, expectedOrders, null);
                    bootupLatch.countDown();
                });
            });
        });
        return expectedOrders;
    }

    private List<TestOrder> getOrders(Side side, WebSocket websocket, List<TestOrder> expectedOrders, VertxTestContext testContext) {
        JsonObject asksRequest = new JsonObject();
        final String fieldSide = side == Side.BID ? "bids" : "asks";
        asksRequest.put("method", fieldSide);
        Buffer encodedRequest = Buffer.buffer(asksRequest.encode());
        websocket.write(encodedRequest);

        List<TestOrder> orders = new LinkedList<>();
        websocket.handler(response -> {
            JsonArray jsonArray = response.toJsonObject().getJsonArray("orders");

            for (Object obj : jsonArray) {
                if (obj instanceof JsonObject jsonObject) {
                    TestOrder thisOrder = jsonObject.mapTo(TestOrder.class);
                    orders.add(thisOrder);
                }
            }
            assertEquals(expectedOrders, orders);

            if (testContext != null) {
                testContext.completeNow();
            }
        });
        return orders;
    }
}
