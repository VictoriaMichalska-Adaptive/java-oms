package weareadaptive.com.cluster.services.infra;

import com.weareadaptive.sbe.*;
import io.aeron.cluster.service.ClientSession;
import org.agrona.MutableDirectBuffer;
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
    @Override
    public void onExecutionResult(ClientSession session, long correlationId, ExecutionResult executionResult)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + ExecutionResultEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));

        executionResultEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);

        messageHeaderEncoder.correlationId(correlationId);
        executionResultEncoder.orderId(executionResult.getOrderId());
        executionResultEncoder.status(executionResult.getStatus().getByte());
        while (session.offer(directBuffer, 0, encodedLength) < 0);
    }

    @Override
    public void onSuccessMessage(ClientSession session, long correlationId)
    {
        final int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + SuccessMessageEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));

        successMessageEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);
        messageHeaderEncoder.correlationId(correlationId);
        successMessageEncoder.status(Status.SUCCESS.getByte());
        while (session.offer(directBuffer, 0, encodedLength) < 0);
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
            while (session.offer(directBuffer, 0, encodedLength) < 0);
        }

        MutableDirectBuffer endOfOrdersBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MessageHeaderEncoder.ENCODED_LENGTH));

        endOfOrdersEncoder.wrapAndApplyHeader(endOfOrdersBuffer, 0, messageHeaderEncoder);
        messageHeaderEncoder.correlationId(correlationId);

        while (session.offer(endOfOrdersBuffer, 0, MessageHeaderEncoder.ENCODED_LENGTH) < 0);
    }

    @Override
    public void onOrderId(ClientSession session, long correlationId, long currentOrderId)
    {
        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + OrderIdEncoder.BLOCK_LENGTH;
        MutableDirectBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength));

        orderIdEncoder.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder);
        messageHeaderEncoder.correlationId(correlationId);
        orderIdEncoder.orderId(currentOrderId);
        while (session.offer(directBuffer, 0, encodedLength) < 0);
    }
}
