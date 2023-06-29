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
        orderbook.placeOrder(15, 10, Side.ASK); // ask

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
        orderbook.placeOrder(15, 10, Side.ASK); // bid

        // act
        final var returnedAsk = orderbook.placeOrder(10, 10, Side.ASK); // non-crossing ask

        // assert
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.RESTING, returnedAsk.getStatus());
        Assertions.assertTrue(orderbook.activeIds.contains(returnedAsk.getOrderId()));
    }

    @Test
    @DisplayName("Crossing BID that will be partially filled is placed and returns its orderId and a PARTIAL status")
    public void placePartialBid() {
        // arrange
        orderbook.placeOrder(10, 10, Side.ASK); // crossing ask

        // act
        final var returnedBid = orderbook.placeOrder(10, 15, Side.BID); // crossing bid

        // assert
        Assertions.assertTrue(returnedBid.getOrderId() > 0);
        Assertions.assertEquals(Status.PARTIAL, returnedBid.getStatus());
        Assertions.assertTrue(orderbook.activeIds.contains(returnedBid.getOrderId()));
    }

    @Test
    @DisplayName("Crossing ASK that will be partially filled is placed and returns its orderId and a PARTIAL status")
    public void placePartialAsk() {
        // arrange
        orderbook.placeOrder(5, 10, Side.BID); // bid

        // act
        final var returnedAsk = orderbook.placeOrder(10, 15, Side.ASK); // crossing ask

        // assert
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.PARTIAL, returnedAsk.getStatus());
        Assertions.assertTrue(orderbook.activeIds.contains(returnedAsk.getOrderId()));
    }

    @Test
    @DisplayName("Crossing BID that will be filled entirely is placed and returns its orderId and a FILLED status")
    public void placeFilledBid() {
        // arrange
        orderbook.placeOrder(10, 10, Side.ASK); // ask

        // act
        final var returnedBid = orderbook.placeOrder(15, 5, Side.BID); // crossing bid

        // assert
        Assertions.assertTrue(returnedBid.getOrderId() > 0);
        Assertions.assertEquals(Status.FILLED, returnedBid.getStatus());
        Assertions.assertFalse(orderbook.activeIds.contains(returnedBid.getOrderId()));
    }

    @Test
    @DisplayName("Multiple BIDs with the same price will be fulfilled in the order they were given")
    public void multipleIdenticalBids() {
        final var firstBid = orderbook.placeOrder(10, 10, Side.BID);
        final var secondBid = orderbook.placeOrder(10, 15, Side.BID);
        final var thirdBid = orderbook.placeOrder(10, 20, Side.BID);
        final var completedAsk = orderbook.placeOrder(10, 10, Side.ASK);
        final var cancelledSecondBid = orderbook.cancelOrder(secondBid.getOrderId());
        final var cancelledThirdBid = orderbook.cancelOrder(thirdBid.getOrderId());
        Assertions.assertEquals(Status.FILLED, completedAsk.getStatus());

        // attempt to cancel
        final var cancelledBid = orderbook.cancelOrder(firstBid.getOrderId());

        // make sure first bid cannot have been cancelled because it has been fulfilled
        Assertions.assertEquals(Status.NONE, cancelledBid.getStatus());
        Assertions.assertEquals(firstBid.getOrderId(), cancelledBid.getOrderId());

        // assert other bids were cancelled and therefore not fulfilled
        Assertions.assertEquals(Status.CANCELLED, cancelledSecondBid.getStatus());
        Assertions.assertEquals(secondBid.getOrderId(), cancelledSecondBid.getOrderId());
        Assertions.assertEquals(Status.CANCELLED, cancelledThirdBid.getStatus());
        Assertions.assertEquals(thirdBid.getOrderId(), cancelledThirdBid.getOrderId());
    }

    @Test
    @DisplayName("Multiple ASKs with the same price will be fulfilled in the order they were given")
    public void multipleIdenticalAsks() {
        final var firstBid = orderbook.placeOrder(10, 10, Side.ASK);
        final var secondBid = orderbook.placeOrder(10, 15, Side.ASK);
        final var thirdBid = orderbook.placeOrder(10, 20, Side.ASK);
        final var completedAsk = orderbook.placeOrder(10, 10, Side.BID);
        final var cancelledSecondBid = orderbook.cancelOrder(secondBid.getOrderId());
        final var cancelledThirdBid = orderbook.cancelOrder(thirdBid.getOrderId());
        Assertions.assertEquals(Status.FILLED, completedAsk.getStatus());

        // attempt to cancel
        final var cancelledBid = orderbook.cancelOrder(firstBid.getOrderId());

        // make sure first bid cannot have been cancelled because it has been fulfilled
        Assertions.assertEquals(Status.NONE, cancelledBid.getStatus());
        Assertions.assertEquals(firstBid.getOrderId(), cancelledBid.getOrderId());

        // assert other bids were cancelled and therefore not fulfilled
        Assertions.assertEquals(Status.CANCELLED, cancelledSecondBid.getStatus());
        Assertions.assertEquals(secondBid.getOrderId(), cancelledSecondBid.getOrderId());
        Assertions.assertEquals(Status.CANCELLED, cancelledThirdBid.getStatus());
        Assertions.assertEquals(thirdBid.getOrderId(), cancelledThirdBid.getOrderId());
    }

    @Test
    @DisplayName("Crossing ASK that will be filled entirely is placed and returns its orderId and a FILLED status")
    public void placeFilledAsk() {
        // arrange
        orderbook.placeOrder(10, 10, Side.BID); // bid

        // act
        final var returnedAsk = orderbook.placeOrder(15, 5, Side.ASK); // crossing ask

        // assert
        Assertions.assertTrue(returnedAsk.getOrderId() > 0);
        Assertions.assertEquals(Status.FILLED, returnedAsk.getStatus());
        Assertions.assertFalse(orderbook.activeIds.contains(returnedAsk.getOrderId()));
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
        Assertions.assertFalse(orderbook.activeIds.contains(returnedBid.getOrderId()));
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
        Assertions.assertFalse(orderbook.activeIds.contains(returnedAsk.getOrderId()));
    }

    @Test
    @DisplayName("Non-existing orderId is used to cancel a BID and returns the orderId and a NONE status")
    public void cancelNonExistingBid() {
        final Long fakeId = 100L;
        final var cancelledBid = orderbook.cancelOrder(fakeId);
        Assertions.assertEquals(fakeId, cancelledBid.getOrderId());
        Assertions.assertEquals(Status.NONE, cancelledBid.getStatus());
        Assertions.assertFalse(orderbook.activeIds.contains(fakeId));
    }

    @Test
    @DisplayName("Non-existing orderId is used to cancel a ASK and returns the orderId and a NONE status")
    public void cancelNonExistingAsk() {
        final Long fakeId = 100L;
        final var cancelledAsk = orderbook.cancelOrder(fakeId);
        // assert
        Assertions.assertEquals(fakeId, cancelledAsk.getOrderId());
        Assertions.assertEquals(Status.NONE, cancelledAsk.getStatus());
        Assertions.assertFalse(orderbook.activeIds.contains(fakeId));
    }

    @Test
    @DisplayName("All orderbook orders are cleared and should be empty when checked, orderId should not be reset.")
    public void clearOrderbook() {
        orderbook.placeOrder(100, 1, Side.ASK);
        orderbook.placeOrder(100, 1, Side.ASK);
        orderbook.placeOrder(100, 1, Side.ASK);
        orderbook.placeOrder(1, 100, Side.BID);
        orderbook.placeOrder(2, 100, Side.BID);
        orderbook.placeOrder(3, 100, Side.BID);
        orderbook.placeOrder(4, 100, Side.BID);
        Assertions.assertEquals(3, orderbook.showAsks().size());
        Assertions.assertEquals(4, orderbook.showBids().size());
        final var currentId = orderbook.currentOrderId;

        orderbook.clear();

        Assertions.assertEquals(0, orderbook.showAsks().size());
        Assertions.assertEquals(0, orderbook.showBids().size());
        Assertions.assertTrue(orderbook.activeIds.isEmpty());
        Assertions.assertEquals(currentId, orderbook.currentOrderId);
    }

    @Test
    @DisplayName("Entire orderbook state is reset, all states should be at initial values or empty.")
    public void resetOrderbook() {
        orderbook.placeOrder(100, 1, Side.ASK);
        orderbook.placeOrder(100, 1, Side.ASK);
        orderbook.placeOrder(100, 1, Side.ASK);
        orderbook.placeOrder(1, 100, Side.BID);
        orderbook.placeOrder(2, 100, Side.BID);
        orderbook.placeOrder(3, 100, Side.BID);
        orderbook.placeOrder(4, 100, Side.BID);
        Assertions.assertEquals(3, orderbook.showAsks().size());
        Assertions.assertEquals(4, orderbook.showBids().size());
        final var currentId = orderbook.currentOrderId;

        orderbook.reset();

        Assertions.assertEquals(0, orderbook.showAsks().size());
        Assertions.assertEquals(0, orderbook.showBids().size());
        Assertions.assertTrue(orderbook.activeIds.isEmpty());
        Assertions.assertNotEquals(currentId, orderbook.currentOrderId);
    }

}
