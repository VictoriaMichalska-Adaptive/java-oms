package com.weareadaptive.cluster.services;

import com.weareadaptive.cluster.ClusterNode;
import com.weareadaptive.cluster.services.oms.OrderbookImpl;
import com.weareadaptive.cluster.services.oms.util.ExecutionResult;
import com.weareadaptive.util.OrderRequest;
import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.weareadaptive.util.Decoder.*;

// todo: potentially abstract away the while-loops
public class OMSService
{
    private final OrderbookImpl orderbook = new OrderbookImpl();
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterNode.class);

    public OMSService()
    {
    }

    public void messageHandler(final ClientSession session, final DirectBuffer buffer, final int offset) {
        final var method = decodeMethod(buffer, offset);
        final var currentOffset = offset + METHOD_NAME_SIZE;
        switch(method) {
            case CLEAR -> clearOrderbook(session, buffer, currentOffset);
            case PLACE -> placeOrder(session, buffer, currentOffset);
            case RESET -> resetOrderbook(session, buffer, currentOffset);
            case CANCEL -> cancelOrder(session, buffer, currentOffset);
        }
    }

    private void placeOrder(final ClientSession session, final DirectBuffer buffer, final int offset)
    {
        /*
         * * Receive Ingress binary encoding and place order in Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        final long id = decodeLongId(buffer, offset);

        final OrderRequest order = decodeOrderRequest(buffer, offset + Long.BYTES);

        LOGGER.info(String.format("%s order is being placed for %d at %f", order.side().toString(), order.size(), order.price()));
        final ExecutionResult executionResult = orderbook.placeOrder(order.price(), order.size(), order.side());

        while (session.offer(encodeExecutionResult(id, executionResult), 0, EXECUTION_RESULT_SIZE) < 0);
    }


    private void cancelOrder(final ClientSession session, final DirectBuffer buffer, final int offset)
    {
        /*
         * * Receive Ingress binary encoding and cancel order in Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        int position = offset;

        long messageId = decodeLongId(buffer, position);

        position += ID_SIZE;
        long orderId = decodeLongId(buffer, position);

        LOGGER.info("Cancelling order " + orderId);
        final ExecutionResult executionResult = orderbook.cancelOrder(orderId);

        while (session.offer(encodeExecutionResult(messageId, executionResult), 0, EXECUTION_RESULT_SIZE) < 0);
    }

    private void clearOrderbook(final ClientSession session, final DirectBuffer buffer, final int offset)
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
        Long messageId = decodeLongId(buffer, offset);

        while (session.offer(encodeSuccessMessage(messageId), 0, SUCCESS_MESSAGE_SIZE) < 0);
    }

    private void resetOrderbook(final ClientSession session, final DirectBuffer buffer, final int offset)
    {
        /*
         * * Receive Ingress binary encoding and reset Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        orderbook.reset();
        Long messageId = decodeLongId(buffer, offset);

        while (session.offer(encodeSuccessMessage(messageId), 0, SUCCESS_MESSAGE_SIZE) < 0);
    }

    public void onTakeSnapshot()
    {
        /*
         * * Encode current orderbook state and offer to SnapshotPublication
         *      - Convert data structures in Orderbook for encoding
         *      - Encode Orderbook state
         *      - Offer to SnapshotPublication
         */
    }

    public void onRestoreSnapshot()
    {
        /*
         * * Decode Snapshot Image and restore Orderbook state
         *      - Decode Snapshot Image encoding into appropriate data structures
         *      - Restore into Orderbook state
         */
    }
}
