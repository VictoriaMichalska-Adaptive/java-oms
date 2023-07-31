package com.weareadaptive.cluster.services.oms;

import com.weareadaptive.cluster.ClusterNode;
import com.weareadaptive.cluster.services.infra.ClusterClientResponder;
import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.cluster.services.oms.util.Method;
import com.weareadaptive.cluster.services.util.CustomHeader;
import com.weareadaptive.cluster.services.util.OrderRequestCommand;
import io.aeron.Image;
import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.weareadaptive.cluster.codec.Codec.*;
import static com.weareadaptive.cluster.codec.SnapshotCodec.decodeOrderbookState;
import static com.weareadaptive.cluster.codec.SnapshotCodec.encodeOrderbookState;
import static com.weareadaptive.util.CodecConstants.ID_SIZE;

public class OMSService
{
    private OrderbookImpl orderbook = new OrderbookImpl();
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterNode.class);
    private final ClusterClientResponder clusterClientResponder;


    public OMSService(ClusterClientResponder clusterClientResponder)
    {
        this.clusterClientResponder = clusterClientResponder;
    }

    public void messageHandler(final ClientSession session, final CustomHeader customHeader, final DirectBuffer buffer, final int offset) {
        final var method = customHeader.getMethod();
        switch(method) {
            case CANCEL -> cancelOrder(session, customHeader.getMessageId(), buffer, offset);
            case PLACE -> placeOrder(session, customHeader.getMessageId(), buffer, offset);
            case RESET -> resetOrderbook(session, customHeader.getMessageId());
            case CLEAR -> clearOrderbook(session, customHeader.getMessageId());
            case ASKS, BIDS -> getOrders(session, customHeader.getMessageId(), method);
            case CURRENT_ORDER_ID -> getCurrentOrderId(session, customHeader.getMessageId());
        }
    }

    private void getCurrentOrderId(ClientSession session, long messageId) {
        final long currentOrderId = orderbook.getCurrentOrderId();
        clusterClientResponder.onOrderId(session, messageId, currentOrderId);
    }

    private void getOrders(ClientSession session, long messageId, Method method)
    {
        LOGGER.info("List of %s orders is being requested".formatted(method.name()));
        switch(method) {
            case ASKS -> clusterClientResponder.onOrders(session, messageId, orderbook.getAsks());
            case BIDS -> clusterClientResponder.onOrders(session, messageId, orderbook.getBids());
            default -> LOGGER.error("This method is does not return a list of Orders");
        }
    }

    private void placeOrder(final ClientSession session, final long messageId, final DirectBuffer buffer, final int offset)
    {
        /*
         * * Receive Ingress binary encoding and place order in Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        final OrderRequestCommand order = decodeOrderRequest(buffer, offset);

        LOGGER.info(String.format("%s order is being placed for %d at %f", order.getSide().toString(), order.getSize(), order.getPrice()));
        final ExecutionResult executionResult = orderbook.placeOrder(order.getPrice(), order.getSize(), order.getSide());

        clusterClientResponder.onExecutionResult(session, messageId, executionResult);
    }


    private void cancelOrder(final ClientSession session, final long correlationId, final DirectBuffer buffer, final int offset)
    {
        /*
         * * Receive Ingress binary encoding and cancel order in Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        int position = offset;

        position += ID_SIZE;
        long orderId = decodeLongId(buffer, position);

        LOGGER.info("Cancelling order " + orderId);
        final ExecutionResult executionResult = orderbook.cancelOrder(orderId);

        clusterClientResponder.onExecutionResult(session, correlationId, executionResult);
    }

    private void clearOrderbook(final ClientSession session, final long correlationId)
    {
        /*
         * * Receive Ingress binary encoding and clear Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        LOGGER.info("Clearing orderbook...");
        orderbook.clear();

        clusterClientResponder.onSuccessMessage(session, correlationId);
    }

    private void resetOrderbook(final ClientSession session, final long correlationId)
    {
        /*
         * * Receive Ingress binary encoding and reset Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        LOGGER.info("Resetting orderbook...");
        orderbook.reset();

        clusterClientResponder.onSuccessMessage(session, correlationId);
    }

    public DirectBuffer onTakeSnapshot()
    {
        /*
         * * Encode current orderbook state and offer to SnapshotPublication
         *      - Convert data structures in Orderbook for encoding
         *      - Encode Orderbook state
         *      - Offer to SnapshotPublication
         */
        return encodeOrderbookState(orderbook.getAsks(), orderbook.getBids(), orderbook.getCurrentOrderId());
    }

    public void onRestoreSnapshot(Image snapshotImage)
    {
        /*
         * * Decode Snapshot Image and restore Orderbook state
         *      - Decode Snapshot Image encoding into appropriate data structures
         *      - Restore into Orderbook state
         */
        orderbook = decodeOrderbookState(snapshotImage);
    }
}
