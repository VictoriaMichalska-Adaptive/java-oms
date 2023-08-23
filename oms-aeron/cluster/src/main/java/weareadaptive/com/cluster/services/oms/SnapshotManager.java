package weareadaptive.com.cluster.services.oms;

import com.weareadaptive.sbe.snapshotting.*;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weareadaptive.com.cluster.services.oms.util.Order;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

public class SnapshotManager implements FragmentHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotManager.class);
    private static final int RETRY_COUNT = 3;
    private boolean snapshotFullyLoaded = false;
    private IdleStrategy idleStrategy;
    public OrderbookImpl orderbook = new OrderbookImpl();

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final OrderIdEncoder orderIdEncoder = new OrderIdEncoder();
    private final OrderIdDecoder orderIdDecoder = new OrderIdDecoder();
    private final EndOfSnapshotEncoder endOfSnapshotEncoder = new EndOfSnapshotEncoder();
    private final AskOrderEncoder askOrderEncoder = new AskOrderEncoder();
    private final AskOrderDecoder askOrderDecoder = new AskOrderDecoder();
    private final BidOrderEncoder bidOrderEncoder = new BidOrderEncoder();
    private final BidOrderDecoder bidOrderDecoder = new BidOrderDecoder();
    private final int askBufferLength = MessageHeaderEncoder.ENCODED_LENGTH + AskOrderEncoder.BLOCK_LENGTH;
    private final int bidBufferLength = MessageHeaderEncoder.ENCODED_LENGTH + BidOrderEncoder.BLOCK_LENGTH;
    private final int currentIdLength = MessageHeaderEncoder.ENCODED_LENGTH + OrderIdEncoder.BLOCK_LENGTH;

    private final MutableDirectBuffer askOrderBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(askBufferLength));
    private final MutableDirectBuffer bidOrderBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(bidBufferLength));
    private final MutableDirectBuffer currentIdBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(currentIdLength));
    private final int endOfSnapshotLength = MessageHeaderEncoder.ENCODED_LENGTH + EndOfSnapshotEncoder.BLOCK_LENGTH;
    private final MutableDirectBuffer endOfSnapshotBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(endOfSnapshotLength));

    public void encodeOrderbookState(ExclusivePublication snapshotPublication, TreeSet<Order> asks, TreeSet<Order> bids, long currentOrderId)
    {
        LOGGER.info("Starting snapshot...");
        offerAskOrders(snapshotPublication, asks);
        offerBidOrders(snapshotPublication, bids);
        offerCurrentId(snapshotPublication, currentOrderId);
        offerEndOfSnapshot(snapshotPublication);
        LOGGER.info("Snapshot complete");
    }

    private void offerEndOfSnapshot(ExclusivePublication publication)
    {
        endOfSnapshotEncoder.wrapAndApplyHeader(endOfSnapshotBuffer, 0, messageHeaderEncoder);

        retryingOffer(publication, endOfSnapshotBuffer, endOfSnapshotLength);
    }

    private void offerCurrentId(final ExclusivePublication publication, final long currentOrderId)
    {
        orderIdEncoder.wrapAndApplyHeader(currentIdBuffer, 0, messageHeaderEncoder);
        orderIdEncoder.orderId(currentOrderId);

        retryingOffer(publication, currentIdBuffer, currentIdLength);
    }

    private void offerAskOrders(ExclusivePublication publication, SortedSet<Order> orders)
    {
        for (Order order : orders) {
            askOrderEncoder.wrapAndApplyHeader(askOrderBuffer, 0, messageHeaderEncoder);

            askOrderEncoder.orderId(order.getOrderId());
            askOrderEncoder.price(order.getPrice());
            askOrderEncoder.size(order.getSize());
            retryingOffer(publication, askOrderBuffer, askBufferLength);
        }
    }
    private void offerBidOrders(ExclusivePublication publication, SortedSet<Order> orders)
    {
        for (Order order : orders) {
            bidOrderEncoder.wrapAndApplyHeader(bidOrderBuffer, 0, messageHeaderEncoder);

            bidOrderEncoder.orderId(order.getOrderId());
            bidOrderEncoder.price(order.getPrice());
            bidOrderEncoder.size(order.getSize());

            retryingOffer(publication, bidOrderBuffer, bidBufferLength);
        }
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
    {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }

        messageHeaderDecoder.wrap(buffer, offset);
        final int templateId = messageHeaderDecoder.templateId();
        final int actingBlockLength = messageHeaderDecoder.blockLength();
        final int actingVersion = messageHeaderDecoder.version();

        final int updatedOffset = offset + messageHeaderDecoder.encodedLength();
        switch (templateId) {
            case AskOrderDecoder.TEMPLATE_ID -> {
                askOrderDecoder.wrap(buffer, updatedOffset, actingBlockLength, actingVersion);
                final long orderId = askOrderDecoder.orderId();
                final double price = askOrderDecoder.price();
                final long size = askOrderDecoder.size();
                orderbook.getAsks().add(new Order(orderId, price, size));
            }
            case BidOrderDecoder.TEMPLATE_ID -> {
                bidOrderDecoder.wrap(buffer, updatedOffset, actingBlockLength, actingVersion);
                final long orderId = bidOrderDecoder.orderId();
                final double price = bidOrderDecoder.price();
                final long size = bidOrderDecoder.size();
                orderbook.getBids().add(new Order(orderId, price, size));
            }
            case OrderIdDecoder.TEMPLATE_ID -> {
                orderIdDecoder.wrap(buffer, updatedOffset, actingBlockLength, actingVersion);
                final long orderId = orderIdDecoder.orderId();
                orderbook.setCurrentOrderId(orderId);
            }
            case EndOfSnapshotDecoder.TEMPLATE_ID -> snapshotFullyLoaded = true;
            default -> LOGGER.warn("Unknown snapshot message template id: {}", templateId);
        }
    }

    public OrderbookImpl loadSnapshot(final Image snapshotImage)
    {
        LOGGER.info("Loading snapshot...");
        snapshotFullyLoaded = false;
        Objects.requireNonNull(idleStrategy, "Idle strategy must be set before loading snapshot");
        idleStrategy.reset();
        while (!snapshotImage.isEndOfStream())
        {
            idleStrategy.idle(snapshotImage.poll(this, 20));
        }

        if (!snapshotFullyLoaded)
        {
            LOGGER.warn("Snapshot load not completed; no end of snapshot marker found");
        }
        LOGGER.info("Snapshot load complete.");

        return orderbook;
    }

    public void setIdleStrategy(final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;
    }

    private void retryingOffer(final ExclusivePublication publication, final DirectBuffer buffer, final int length)
    {
        final int offset = 0;
        int retries = 0;
        do
        {
            idleStrategy.reset();
            final long result = publication.offer(buffer, offset, length);
            if (result >= 0L)
            {
                return;
            }
            else if (result == Publication.ADMIN_ACTION || result == Publication.BACK_PRESSURED)
            {
                LOGGER.warn("backpressure or admin action on snapshot");
            }
            else if (result == Publication.NOT_CONNECTED || result == Publication.MAX_POSITION_EXCEEDED)
            {
                LOGGER.error("unexpected publication state on snapshot: {}", result);
                return;
            }
            idleStrategy.idle();
            retries += 1;
        }
        while (retries < RETRY_COUNT);

        LOGGER.error("failed to offer snapshot within {} retries", RETRY_COUNT);
    }
}