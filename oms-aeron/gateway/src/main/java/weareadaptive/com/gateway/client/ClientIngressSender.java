package weareadaptive.com.gateway.client;

import com.weareadaptive.sbe.*;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weareadaptive.com.cluster.services.oms.util.Method;
import weareadaptive.com.cluster.services.oms.util.Side;

import java.nio.ByteBuffer;

public class ClientIngressSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientIngressSender.class);
    final OrderRequestEncoder orderRequestEncoder = new OrderRequestEncoder();
    final CancelRequestEncoder cancelOrderEncoder = new CancelRequestEncoder();
    final private AsksRequestEncoder asksRequestEncoder = new AsksRequestEncoder();
    final private BidsRequestEncoder bidsRequestEncoder = new BidsRequestEncoder();
    final private ResetRequestEncoder resetRequestEncoder = new ResetRequestEncoder();
    final private ClearRequestEncoder clearRequestEncoder = new ClearRequestEncoder();
    final private CurrentIdRequestEncoder currentIdRequestEncoder = new CurrentIdRequestEncoder();
    final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    AeronCluster aeronCluster;

    public ClientIngressSender(final AeronCluster aeronCluster)
    {
        this.aeronCluster = aeronCluster;
    }

    public void sendMessageToCluster(MutableDirectBuffer buffer, int length) {
        while (aeronCluster.offer(buffer, 0, length) < 0);
    }

    private void setHeaderEncoder(final MutableDirectBuffer buffer, final long correlationId) {
        headerEncoder.wrap(buffer, 0);
        headerEncoder.correlationId(correlationId);
    }

    public void sendOrderRequestToCluster(final long correlationId, final double price, final long size, final Side side) {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + OrderRequestEncoder.BLOCK_LENGTH;
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(encodedLength);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);
        orderRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);

        orderRequestEncoder.price(price);
        orderRequestEncoder.size(size);
        orderRequestEncoder.side(side.getByte());

        sendMessageToCluster(directBuffer, encodedLength);
    }

    public void sendCancelOrderToCluster(final long correlationId, final long orderId)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + CancelRequestEncoder.BLOCK_LENGTH;
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(encodedLength);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);
        cancelOrderEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
        cancelOrderEncoder.orderId(orderId);

        sendMessageToCluster(directBuffer, encodedLength);
    }

    public void sendHeaderMessageToCluster(long correlationId, Method method)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH;
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(encodedLength);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);

        switch (method) {
            case CLEAR -> clearRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case RESET -> resetRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case BIDS -> bidsRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case ASKS -> asksRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case CURRENT_ORDER_ID -> currentIdRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
        }
        sendMessageToCluster(directBuffer, encodedLength);
    }
}
