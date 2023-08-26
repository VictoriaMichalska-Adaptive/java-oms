package weareadaptive.com.cluster.services.infra;

import com.weareadaptive.sbe.*;
import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weareadaptive.com.cluster.services.oms.util.ExecutionResult;
import weareadaptive.com.cluster.services.oms.util.Order;
import weareadaptive.com.cluster.services.oms.util.Status;

import java.nio.ByteBuffer;
import java.util.TreeSet;

public class ClusterClientResponderImpl implements ClusterClientResponder
{
    private final Logger LOGGER = LoggerFactory.getLogger(ClusterClientResponderImpl.class);
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ExecutionResultEncoder executionResultEncoder = new ExecutionResultEncoder();
    private final SuccessMessageEncoder successMessageEncoder = new SuccessMessageEncoder();
    private final OrderIdEncoder orderIdEncoder = new OrderIdEncoder();
    private final OrderEncoder orderEncoder = new OrderEncoder();
    private final EndOfOrdersEncoder endOfOrdersEncoder = new EndOfOrdersEncoder();
    private IdleStrategy idleStrategy = new SleepingIdleStrategy();
    @Override
    public void onExecutionResult(ClientSession session, long correlationId, ExecutionResult executionResult)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + ExecutionResultEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));

        executionResultEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);

        messageHeaderEncoder.correlationId(correlationId);
        executionResultEncoder.orderId(executionResult.getOrderId());
        executionResultEncoder.status(executionResult.getStatus().getByte());
        LOGGER.info("Sending executionResult");
        sendMessageToSession(session, directBuffer, encodedLength);
    }

    @Override
    public void onSuccessMessage(ClientSession session, long correlationId)
    {
        final int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + SuccessMessageEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));

        successMessageEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);
        messageHeaderEncoder.correlationId(correlationId);
        successMessageEncoder.status(Status.SUCCESS.getByte());
        sendMessageToSession(session, directBuffer, encodedLength);
    }

    @Override
    public void onOrders(ClientSession session, long correlationId, TreeSet<Order> orders)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + OrderEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));
        orderEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);
        messageHeaderEncoder.correlationId(correlationId);

        for (Order order : orders)
        {
            orderEncoder.orderId(order.getOrderId());
            orderEncoder.price(order.getPrice());
            orderEncoder.size(order.getSize());
            sendMessageToSession(session, directBuffer, encodedLength);
        }

        MutableDirectBuffer endOfOrdersBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MessageHeaderEncoder.ENCODED_LENGTH));

        endOfOrdersEncoder.wrapAndApplyHeader(endOfOrdersBuffer, 0, messageHeaderEncoder);
        messageHeaderEncoder.correlationId(correlationId);
        sendMessageToSession(session, endOfOrdersBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
    }

    @Override
    public void onOrderId(ClientSession session, long correlationId, long currentOrderId)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + OrderIdEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));

        orderIdEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);
        messageHeaderEncoder.correlationId(correlationId);
        orderIdEncoder.orderId(currentOrderId);
        sendMessageToSession(session, directBuffer, encodedLength);
    }

    @Override
    public void setIdleStrategy(IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;
    }

    public void sendMessageToSession(ClientSession session, DirectBuffer directBuffer, int encodedLength) {
        // todo: adjust according to wehther or not backpressure?? idk
        final int offset = 0;
        int retries = 0;
        int RETRY_COUNT = 3;
        do
        {
            idleStrategy.reset();
            final long result = session.offer(directBuffer, offset, encodedLength);
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
