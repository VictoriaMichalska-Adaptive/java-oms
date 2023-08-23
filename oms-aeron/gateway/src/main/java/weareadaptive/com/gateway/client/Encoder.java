package weareadaptive.com.gateway.client;

import com.weareadaptive.sbe.*;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import weareadaptive.com.cluster.services.oms.util.Method;
import weareadaptive.com.cluster.services.oms.util.Side;

import java.nio.ByteBuffer;

public class Encoder
{
    final private MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    protected final int CANCEL_ORDER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + CancelRequestEncoder.BLOCK_LENGTH;
    final private CancelRequestEncoder cancelOrderEncoder = new CancelRequestEncoder();
    protected final int ORDER_REQUEST_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + OrderRequestEncoder.BLOCK_LENGTH;
    final private OrderRequestEncoder orderRequestEncoder = new OrderRequestEncoder();
    protected final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    final private AsksRequestEncoder asksRequestEncoder = new AsksRequestEncoder();
    final private BidsRequestEncoder bidsRequestEncoder = new BidsRequestEncoder();
    final private ResetRequestEncoder resetRequestEncoder = new ResetRequestEncoder();
    final private ClearRequestEncoder clearRequestEncoder = new ClearRequestEncoder();
    final private CurrentIdRequestEncoder currentIdRequestEncoder = new CurrentIdRequestEncoder();

    private void setHeaderEncoder(final MutableDirectBuffer buffer, final long correlationId) {
        headerEncoder.wrap(buffer, 0);
        headerEncoder.correlationId(correlationId);
    }

    protected MutableDirectBuffer encodeCancelOrder(long correlationId, long orderId)
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(CANCEL_ORDER_LENGTH);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);
        cancelOrderEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
        cancelOrderEncoder.orderId(orderId);

        return directBuffer;
    }

    protected MutableDirectBuffer encodeOrderRequest(long correlationId, double price, long size, Side side) {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(ORDER_REQUEST_LENGTH);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);
        orderRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);

        orderRequestEncoder.price(price);
        orderRequestEncoder.size(size);
        orderRequestEncoder.side(side.getByte());

        return directBuffer;
    }

    public MutableDirectBuffer encodeHeaderMessage(long correlationId, Method method)
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(HEADER_LENGTH);
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        setHeaderEncoder(directBuffer, correlationId);

        switch (method) {
            case CLEAR -> clearRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case RESET -> resetRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case BIDS -> bidsRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case ASKS -> asksRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
            case CURRENT_ORDER_ID -> currentIdRequestEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder);
        }

        return directBuffer;
    }
}
