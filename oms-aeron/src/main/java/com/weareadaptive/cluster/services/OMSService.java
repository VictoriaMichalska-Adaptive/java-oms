package com.weareadaptive.cluster.services;

import com.weareadaptive.cluster.services.oms.OrderbookImpl;
import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;

public class OMSService
{
    private final OrderbookImpl orderbook = new OrderbookImpl();

    public OMSService()
    {
    }

    private void placeOrder(final ClientSession session, final DirectBuffer buffer, final int offset)
    {
        /**
         * * Receive Ingress binary encoding and place order in Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
    }

    private void cancelOrder(final ClientSession session, final DirectBuffer buffer, final int offset)
    {
        /**
         * * Receive Ingress binary encoding and cancel order in Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
    }

    private void clearOrderbook(final ClientSession session, final DirectBuffer buffer, final int offset)
    {
        /**
         * * Receive Ingress binary encoding and clear Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
    }

    private void resetOrderbook(final ClientSession session, final DirectBuffer buffer, final int offset)
    {
        /**
         * * Receive Ingress binary encoding and reset Orderbook
         *      - Decode buffer
         *      - Perform business logic
         *      - Encode a response
         *      - Offer Egress back to cluster client
         */
    }

    public void onTakeSnapshot()
    {
        /**
         * * Encode current orderbook state and offer to SnapshotPublication
         *      - Convert data structures in Orderbook for encoding
         *      - Encode Orderbook state
         *      - Offer to SnapshotPublication
         */
    }

    public void onRestoreSnaptshot()
    {
        /**
         * * Decode Snapshot Image and restore Orderbook state
         *      - Decode Snapshot Image encoding into appropriate data structures
         *      - Restore into Orderbook state
         */
    }
}
