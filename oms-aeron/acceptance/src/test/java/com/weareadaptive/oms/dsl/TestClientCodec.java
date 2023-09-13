package com.weareadaptive.oms.dsl;

import com.weareadaptive.sbe.ExecutionResultDecoder;
import com.weareadaptive.sbe.OrderDecoder;
import com.weareadaptive.sbe.OrderIdDecoder;
import com.weareadaptive.sbe.SuccessMessageDecoder;
import org.agrona.DirectBuffer;
import weareadaptive.com.cluster.services.oms.util.ExecutionResult;
import weareadaptive.com.cluster.services.oms.util.Order;
import weareadaptive.com.cluster.services.oms.util.Status;

public class TestClientCodec
{
    private final OrderIdDecoder orderIdDecoder = new OrderIdDecoder();
    private final ExecutionResultDecoder executionResultDecoder = new ExecutionResultDecoder();
    private final SuccessMessageDecoder successMessageDecoder = new SuccessMessageDecoder();
    private final OrderDecoder orderDecoder = new OrderDecoder();

    protected ExecutionResult getExecutionResult(final DirectBuffer buffer, final int offset, final int actingBlockLength,
                                                 final int actingVersion)
    {
        executionResultDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final long orderId = executionResultDecoder.orderId();
        final Status status = Status.fromByteValue((byte) executionResultDecoder.status());
        return new ExecutionResult(orderId, status);
    }

    protected Status getSuccessMessage(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        successMessageDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        return Status.fromByteValue((byte) successMessageDecoder.status());
    }

    protected long getOrderId(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        orderIdDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        return orderIdDecoder.orderId();
    }

    protected Order getOrder(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        orderDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        return new Order(orderDecoder.orderId(), orderDecoder.price(), orderDecoder.size());
    }
}
