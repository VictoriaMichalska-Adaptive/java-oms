package com.weareadaptive.oms;

import com.weareadaptive.oms.util.Side;
import com.weareadaptive.oms.util.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class OrderbookTest {
    private OrderbookImpl orderbook;

    @BeforeEach
    void setUp() {
        this.orderbook = new OrderbookImpl();
    }

    @Test
    @DisplayName("Non-crossing BID is placed, and returns its orderId and a RESTING status")
    public void placeRestingBid() {
        // arrange
        orderbook.placeOrder(15, 10, Side.BID); // crossing bid

        // act
        final var returnedBid = orderbook.placeOrder(10, 10, Side.BID); // non-crossing bid

        // assert
        Assertions.assertTrue(returnedBid.getOrderId() > 0);
        Assertions.assertEquals(Status.RESTING, returnedBid.getStatus());
    }

    @Test
    @DisplayName("Non-crossing ASK is placed, and returns its orderId and a RESTING status")
    public void placeRestingAsk() {
        // arrange
        orderbook.placeOrder(15, 10, Side.ASK); // crossing ask

        // act
        final var returnedAsk = orderbook.placeOrder(10, 10, Side.ASK); // non-crossing ask

        // assert
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.RESTING, returnedAsk.getStatus());
    }

    @Test
    @DisplayName("Crossing BID that will be partially filled is placed and returns its orderId and a PARTIAL status")
    public void placePartialBid() {
        // arrange
        orderbook.placeOrder(15, 10, Side.ASK); // crossing ask

        // act
        final var returnedBid = orderbook.placeOrder(10, 15, Side.BID); // crossing bid

        // assert
        Assertions.assertTrue(returnedBid.getOrderId() > 0);
        Assertions.assertEquals(Status.PARTIAL, returnedBid.getStatus());
    }

    @Test
    @DisplayName("Crossing ASK that will be partially filled is placed and returns its orderId and a PARTIAL status")
    public void placePartialAsk() {
        // arrange
        orderbook.placeOrder(15, 10, Side.BID); // crossing bid

        // act
        final var returnedAsk = orderbook.placeOrder(10, 15, Side.ASK); // crossing ask

        // assert
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.PARTIAL, returnedAsk.getStatus());
    }

    @Test
    @DisplayName("Crossing BID that will be filled entirely is placed and returns its orderId and a FILLED status")
    public void placeFilledBid() {
        // arrange
        orderbook.placeOrder(15, 10, Side.ASK); // crossing ask

        // act
        final var returnedBid = orderbook.placeOrder(10, 10, Side.BID); // crossing bid

        // assert
        Assertions.assertTrue(returnedBid.getOrderId() > 0);
        Assertions.assertEquals(Status.FILLED, returnedBid.getStatus());
    }

    @Test
    @DisplayName("Crossing ASK that will be filled entirely is placed and returns its orderId and a FILLED status")
    public void placeFilledAsk() {
        // arrange
        orderbook.placeOrder(15, 10, Side.BID); // crossing bid

        // act
        final var returnedAsk = orderbook.placeOrder(10, 10, Side.ASK); // crossing ask

        // assert
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.FILLED, returnedAsk.getStatus());
    }

    @Test
    @DisplayName("BID is cancelled and returns its orderId and a CANCELLED status")
    public void cancelBid() {
        // arrange
        final var returnedBid = orderbook.placeOrder(10, 15, Side.BID);
        // act
        final var cancelledBid = orderbook.cancelOrder(returnedBid.getOrderId());
        // assert
        Assertions.assertEquals(returnedBid.getOrderId(), cancelledBid.getOrderId());
        Assertions.assertEquals(Status.CANCELLED, cancelledBid.getStatus());
    }

    @Test
    @DisplayName("ASK is cancelled and returns its orderId and a CANCELLED status")
    public void cancelAsk() {
        // arrange
        final var returnedAsk = orderbook.placeOrder(10, 15, Side.ASK);
        // act
        final var cancelledAsk = orderbook.cancelOrder(returnedAsk.getOrderId());
        // assert
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
