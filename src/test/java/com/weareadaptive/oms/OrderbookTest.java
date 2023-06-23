package com.weareadaptive.oms;

import com.weareadaptive.oms.util.Side;
import com.weareadaptive.oms.util.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class OrderbookTest {
    private OrderbookImpl orderbook;
    private double originalBidPrice = 15;
    private long originalBidSize = 3;

    @BeforeEach
    void setUp() {
        this.orderbook = new OrderbookImpl();
        orderbook.placeOrder(originalBidPrice, originalBidSize, Side.BID); // create original highest bid
    }

    @Test
    @DisplayName("Non-crossing BID is placed, and returns its orderId and a RESTING status")
    public void placeRestingBid() {
        final var returnedBid = orderbook.placeOrder(originalBidPrice - 5, originalBidSize, Side.BID);
        Assertions.assertTrue(returnedBid.getOrderId() > 0);
        Assertions.assertEquals(Status.RESTING, returnedBid.getStatus());
    }

    @Test
    @DisplayName("Non-crossing ASK is placed, and returns its orderId and a RESTING status")
    public void placeRestingAsk() {
        final var returnedAsk = orderbook.placeOrder(originalBidPrice - 5, originalBidSize, Side.ASK);
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.RESTING, returnedAsk.getStatus());
    }

    @Test
    @DisplayName("Crossing BID that will be partially filled is placed and returns its orderId and a PARTIAL status")
    public void placePartialBid() {
        final var returnedBid = orderbook.placeOrder(originalBidPrice + 5, originalBidSize, Side.BID);
        Assertions.assertTrue(returnedBid.getOrderId() > 0);
        Assertions.assertEquals(Status.PARTIAL, returnedBid.getStatus());
    }

    @Test
    @DisplayName("Crossing ASK that will be partially filled is placed and returns its orderId and a PARTIAL status")
    public void placePartialAsk() {
        final var returnedAsk = orderbook.placeOrder(originalBidPrice + 5, originalBidSize, Side.ASK);
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.PARTIAL, returnedAsk.getStatus());
    }

    @Test
    @DisplayName("Crossing BID that will be filled entirely is placed and returns its orderId and a FILLED status")
    public void placeFilledBid() {
        final var returnedBid = orderbook.placeOrder(originalBidPrice + 5, originalBidSize, Side.BID);
        Assertions.assertTrue(returnedBid.getOrderId() > 0);
        Assertions.assertEquals(Status.FILLED, returnedBid.getStatus());
    }

    @Test
    @DisplayName("Crossing ASK that will be filled entirely is placed and returns its orderId and a FILLED status")
    public void placeFilledAsk() {
        final var returnedAsk = orderbook.placeOrder(originalBidPrice + 5, originalBidSize, Side.ASK);
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.FILLED, returnedAsk.getStatus());
    }

    @Test
    @DisplayName("BID is cancelled and returns its orderId and a CANCELLED status")
    public void cancelBid() {
        final var returnedBid = orderbook.placeOrder(originalBidPrice + 5, originalBidSize, Side.BID);
        final var cancelledBid = orderbook.cancelOrder(returnedBid.getOrderId());
        Assertions.assertEquals(returnedBid.getOrderId(), cancelledBid.getOrderId());
        Assertions.assertEquals(Status.CANCELLED, cancelledBid.getStatus());
    }

    @Test
    @DisplayName("ASK is cancelled and returns its orderId and a CANCELLED status")
    public void cancelAsk() {
        final var returnedAsk = orderbook.placeOrder(originalBidPrice + 5, originalBidSize, Side.ASK);
        final var cancelledAsk = orderbook.cancelOrder(returnedAsk.getOrderId());
        Assertions.assertEquals(returnedAsk.getOrderId(), cancelledAsk.getOrderId());
        Assertions.assertEquals(Status.CANCELLED, cancelledAsk.getStatus());
    }

    @Test
    @DisplayName("Non-existing orderId is used to cancel a BID and returns the orderId and a NONE status")
    public void cancelNonExistingBid() {}

    @Test
    @DisplayName("Non-existing orderId is used to cancel a ASK and returns the orderId and a NONE status")
    public void cancelNonExistingAsk() {}

    @Test
    @DisplayName("All orderbook orders are cleared and should be empty when checked, orderId should not be reset.")
    public void clearOrderbook() {}

    @Test
    @DisplayName("Entire orderbook state is reset, all states should be at initial values or empty.")
    public void resetOrderbook() {}

}
