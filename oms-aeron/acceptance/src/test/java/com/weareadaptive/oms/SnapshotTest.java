package com.weareadaptive.oms;

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
import weareadaptive.com.cluster.ClusterNode;
import weareadaptive.com.cluster.services.oms.util.Side;
import weareadaptive.com.gateway.ws.command.OrderCommand;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.core.Vertx.vertx;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class SnapshotTest
{
    private Deployment deployment;
    private HttpClient vertxClient;

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
    public void clusterCanRecoverToStateFromSnapshot(final VertxTestContext testContext) throws Throwable
    {
        // todo: research awaitability
        AtomicReference<List<TestOrder>> asks = new AtomicReference<>(null);
        AtomicReference<List<TestOrder>> bids = new AtomicReference<>(null);
        AtomicReference<Long> currentId = new AtomicReference<>(null);

        // set up non-default state
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            OrderCommand bidOrder = new OrderCommand(1, 100, Side.BID);
            bids.set(makeTwoOrdersAndCheckThatTheyOccurred(bidOrder));
        });

        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            OrderCommand askOrder = new OrderCommand(100,1, Side.ASK);
            asks.set(makeTwoOrdersAndCheckThatTheyOccurred(askOrder));
        });

        await().until(() -> asks.get().size() + bids.get().size() == 4);

        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderIdRequest = new JsonObject();
            orderIdRequest.put("method", "orderId");
            Buffer getCurrentId = Buffer.buffer(orderIdRequest.encode());
            websocket.write(getCurrentId);

            websocket.handler(response -> currentId.set(response.toJsonObject().getLong("orderId")));
        });

        await().until(() -> currentId.get() != null);
        // todo: testing dsl??
        // snapshot, shut down, and reboot
        final boolean snapshot = ClusterTool.snapshot(deployment.getNodes().get(deployment.getLeaderId()).getClusterDir(), System.out);
        await().until(() -> snapshot);
        deployment.shutdownCluster();
        await().until(() -> deployment.getNodes().values().stream().noneMatch(ClusterNode::isActive));
        deployment.shutdownGateway();
        await().until(() -> !deployment.getGateway().isActive());
        deployment.startSingleNodeCluster(false);
        await().until(() -> deployment.getNodes().values().stream().anyMatch(ClusterNode::isActive));
        deployment.startGateway(1);
        await().until(() -> deployment.getGateway().isActive());

        // assert same state as original
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(webSocket -> {
            final JsonObject orderIdRequest = new JsonObject();
            orderIdRequest.put("method", "orderId");
            final Buffer getCurrentId = Buffer.buffer(orderIdRequest.encode());
            webSocket.write(getCurrentId);

            webSocket.handler(response -> assertEquals(currentId.get(), response.toJsonObject().getLong("orderId")));
        });
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(webSocket -> getOrders(Side.ASK, webSocket, asks.get(), null));
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(webSocket -> getOrders(Side.BID, webSocket, bids.get(), testContext));

        assertTrue(testContext.awaitCompletion(30, TimeUnit.SECONDS));

    }

    private List<TestOrder> makeTwoOrdersAndCheckThatTheyOccurred(OrderCommand orderCommand)
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
                });
            });
        });
        return expectedOrders;
    }

    private void getOrders(Side side, WebSocket websocket, List<TestOrder> expectedOrders, VertxTestContext testContext) {
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
    }
}
