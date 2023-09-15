package com.weareadaptive.oms;

import com.weareadaptive.oms.dsl.TestClientDsl;
import com.weareadaptive.oms.util.TestOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import weareadaptive.com.cluster.services.oms.util.Order;
import weareadaptive.com.cluster.services.oms.util.Side;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterLogicTest
{
    final Deployment deployment = new Deployment();
    TestClientDsl testClientDsl;
    final int maxNodes = 3;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        deployment.startCluster(maxNodes, true);
        deployment.startTestClientDsl(maxNodes);
        testClientDsl = deployment.getTestClientDsl();
    }

    @AfterEach
    void tearDown()
    {
        deployment.close();
    }

    @Test
    public void placeAskOrder()
    {
        final double price = 10;
        final long size = 5;
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(price, size, Side.ASK);

        List<Order> expectedAsks = new ArrayList<>();
        expectedAsks.add(new Order(testOrder.orderId(), price, size));

        List<Order> actualAsks = testClientDsl.requestAndReceiveAllAsks();
        assertEquals(expectedAsks, actualAsks);
    }

    @Test
    public void placeBidOrder()
    {
        final double price = 10;
        final long size = 5;
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(price, size, Side.BID);

        List<Order> expectedAsks = new ArrayList<>();
        expectedAsks.add(new Order(testOrder.orderId(), price, size));

        List<Order> actualAsks = testClientDsl.requestAndReceiveAllBids();
        assertEquals(expectedAsks, actualAsks);
    }

    @Test
    public void requestEmptyList()
    {
        List<Order> expectedAsks = new ArrayList<>();

        List<Order> actualAsks = testClientDsl.requestAndReceiveAllBids();
        assertEquals(expectedAsks, actualAsks);
    }

    @Test
    public void placeBidOrderAndCancelOrder()
    {
        final double price = 10;
        final long size = 5;
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(price, size, Side.BID);
        List<Order> orders = testClientDsl.requestAndReceiveAllBids();
        assertEquals(1, orders.size());

        testClientDsl.requestCancelOrderAndAwaitResult(testOrder.orderId());
        List<Order> activeOrders = testClientDsl.requestAndReceiveAllBids();
        assertTrue(activeOrders.isEmpty());
    }

    @Test
    public void placeTwoAskOrdersAndCancelOneOrder()
    {
        final double price = 10;
        final long size = 5;
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(price, size, Side.ASK);
        testClientDsl.placeOrderAndAwaitResult(price, size, Side.ASK);
        List<Order> orders = testClientDsl.requestAndReceiveAllAsks();
        assertEquals(2, orders.size());

        testClientDsl.requestCancelOrderAndAwaitResult(testOrder.orderId());
        List<Order> activeOrders = testClientDsl.requestAndReceiveAllAsks();
        assertEquals(1, activeOrders.size());
    }

    @Test
    public void placeTwoBidOrdersAndCancelOneOrder()
    {
        final double price = 10;
        final long size = 5;
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(price, size, Side.BID);
        testClientDsl.placeOrderAndAwaitResult(price, size, Side.BID);
        List<Order> orders = testClientDsl.requestAndReceiveAllBids();
        assertEquals(2, orders.size());

        testClientDsl.requestCancelOrderAndAwaitResult(testOrder.orderId());
        List<Order> activeOrders = testClientDsl.requestAndReceiveAllBids();
        assertEquals(1, activeOrders.size());
    }

    @Test
    public void placeOneBidAndOneAskOrderAndCancelOneBidOrder()
    {
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(10, 100, Side.BID);
        testClientDsl.placeOrderAndAwaitResult(100, 10, Side.ASK);
        List<Order> orders = testClientDsl.requestAndReceiveAllBids();
        assertEquals(1, orders.size());

        testClientDsl.requestCancelOrderAndAwaitResult(testOrder.orderId());
        List<Order> activeOrders = testClientDsl.requestAndReceiveAllBids();
        assertEquals(0, activeOrders.size());
    }

    @Test
    public void placeAskOrderAndCancelOrder()
    {
        final double price = 10;
        final long size = 5;
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(price, size, Side.ASK);

        testClientDsl.requestCancelOrderAndAwaitResult(testOrder.orderId());
        List<Order> activeOrders = testClientDsl.requestAndReceiveAllBids();

        assertTrue(activeOrders.isEmpty());
    }

    @Test
    public void placeOneOrderAndGetOrderId()
    {
        final double price = 10;
        final long size = 5;
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(price, size, Side.ASK);

        assertEquals(testOrder.orderId() + 1, testClientDsl.requestCurrentIdAndAwaitCurrentOrderId());
    }

    @Test
    public void placeMultipleOrdersAndGetOrderId()
    {
        final int price = 10;
        final int size = 100;
        testClientDsl.placeOrderAndAwaitResult(price, size, Side.ASK);
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(size, price, Side.BID);

        assertEquals(testOrder.orderId() + 1, testClientDsl.requestCurrentIdAndAwaitCurrentOrderId());
    }

    @Test
    public void clearingOrderbookAfterMultipleOrders()
    {
        testClientDsl.placeOrderAndAwaitResult(100, 10, Side.ASK);
        testClientDsl.placeOrderAndAwaitResult(10, 100, Side.BID);
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(10, 100, Side.BID);

        assertEquals(testOrder.orderId() + 1, testClientDsl.requestCurrentIdAndAwaitCurrentOrderId());
        assertEquals(1, testClientDsl.requestAndReceiveAllAsks().size());
        assertEquals(2, testClientDsl.requestAndReceiveAllBids().size());

        testClientDsl.clearOrderbook();
        assertEquals(3, testClientDsl.requestCurrentIdAndAwaitCurrentOrderId());
        assertEquals(0, testClientDsl.requestAndReceiveAllAsks().size());
        assertEquals(0, testClientDsl.requestAndReceiveAllBids().size());
    }

    @Test
    public void resettingOrderbookAfterMultipleOrders()
    {
        testClientDsl.placeOrderAndAwaitResult(100, 10, Side.ASK);
        testClientDsl.placeOrderAndAwaitResult(10, 100, Side.BID);
        TestOrder testOrder = testClientDsl.placeOrderAndAwaitResult(10, 100, Side.BID);

        assertEquals(testOrder.orderId() + 1, testClientDsl.requestCurrentIdAndAwaitCurrentOrderId());
        assertEquals(1, testClientDsl.requestAndReceiveAllAsks().size());
        assertEquals(2, testClientDsl.requestAndReceiveAllBids().size());

        testClientDsl.resetOrderbook();
        assertEquals(0, testClientDsl.requestCurrentIdAndAwaitCurrentOrderId());
        assertEquals(0, testClientDsl.requestAndReceiveAllAsks().size());
        assertEquals(0, testClientDsl.requestAndReceiveAllBids().size());
    }
}
