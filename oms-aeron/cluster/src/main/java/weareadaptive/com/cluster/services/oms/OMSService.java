package weareadaptive.com.cluster.services.oms;

import com.weareadaptive.sbe.*;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weareadaptive.com.cluster.ClusterNode;
import weareadaptive.com.cluster.services.infra.ClusterClientResponder;
import weareadaptive.com.cluster.services.oms.util.ExecutionResult;
import weareadaptive.com.cluster.services.oms.util.Method;
import weareadaptive.com.cluster.services.oms.util.Side;

public class OMSService
{
    private final SnapshotManager snapshotManager = new SnapshotManager();
    private OrderbookImpl orderbook = new OrderbookImpl();
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterNode.class);
    private final OrderRequestDecoder orderRequestDecoder = new OrderRequestDecoder();
    private final CancelRequestDecoder cancelRequestDecoder = new CancelRequestDecoder();
    private final ClusterClientResponder clusterClientResponder;


    public OMSService(ClusterClientResponder clusterClientResponder, IdleStrategy idleStrategy)
    {
        snapshotManager.setIdleStrategy(idleStrategy);
        this.clusterClientResponder = clusterClientResponder;
    }

    public void messageHandler(final ClientSession session, final long correlationId, final int templateId, final DirectBuffer buffer, final int offset,
                               int actingBlockLength, int actingVersion) {
        switch(templateId) {
            case CancelRequestDecoder.TEMPLATE_ID -> cancelOrder(session, correlationId, buffer, offset, actingBlockLength, actingVersion);
            case OrderRequestDecoder.TEMPLATE_ID -> placeOrder(session, correlationId, buffer, offset, actingBlockLength, actingVersion);
            case ResetRequestDecoder.TEMPLATE_ID -> resetOrderbook(session, correlationId);
            case ClearRequestDecoder.TEMPLATE_ID -> clearOrderbook(session, correlationId);
            case AsksRequestDecoder.TEMPLATE_ID  -> getOrders(session, correlationId, Method.ASKS);
            case BidsRequestDecoder.TEMPLATE_ID  -> getOrders(session, correlationId, Method.BIDS);
            case CurrentIdRequestDecoder.TEMPLATE_ID -> getCurrentOrderId(session, correlationId);
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

    private void placeOrder(final ClientSession session, final long messageId, final DirectBuffer buffer, final int offset,
                            int actingBlockLength, int actingVersion)
    {
        /*
         * * Receive Ingress binary encoding and place order in Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        orderRequestDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final double orderPrice = orderRequestDecoder.price();
        final long orderSize = orderRequestDecoder.size();
        final Side orderSide = Side.fromByteValue((byte) orderRequestDecoder.side());

        LOGGER.info(String.format("%s order is being placed for %d at %f", orderSide, orderSize, orderPrice));
        final ExecutionResult executionResult = orderbook.placeOrder(orderPrice, orderSize, orderSide);

        clusterClientResponder.onExecutionResult(session, messageId, executionResult);
    }


    private void cancelOrder(final ClientSession session, final long correlationId, final DirectBuffer buffer, final int offset,
                             int actingBlockLength, int actingVersion)
    {
        /*
         * * Receive Ingress binary encoding and cancel order in Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
        cancelRequestDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final long orderId = cancelRequestDecoder.orderId();

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

    public void onTakeSnapshot(ExclusivePublication snapshotPublication)
    {
        /*
         * * Encode current orderbook state and offer to SnapshotPublication
         *      - Convert data structures in Orderbook for encoding
         *      - Encode Orderbook state
         *      - Offer to SnapshotPublication
         */
        snapshotManager.encodeOrderbookState(snapshotPublication, orderbook.getAsks(), orderbook.getBids(), orderbook.getCurrentOrderId());
    }

    public void onRestoreSnapshot(Image snapshotImage)
    {
        /*
         * * Decode Snapshot Image and restore Orderbook state
         *      - Decode Snapshot Image encoding into appropriate data structures
         *      - Restore into Orderbook state
         */
        orderbook = snapshotManager.loadSnapshot(snapshotImage);
    }
}
