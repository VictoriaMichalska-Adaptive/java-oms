package com.weareadaptive.oms.dsl;

import com.weareadaptive.oms.util.TestOrder;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import weareadaptive.com.cluster.services.oms.util.*;

import java.util.List;

import static org.awaitility.Awaitility.await;

public class TestClientDsl implements AutoCloseable
{
    private TestGatewayAgent clientAgent;
    private AgentRunner clientAgentRunner;
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
    private final int maxNodes;


    public TestClientDsl(final int maxNodes)
    {
        this.maxNodes = maxNodes;
    }

    public void startClient()
    {
        clientAgent = new TestGatewayAgent(maxNodes);
        clientAgentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace,
                null, clientAgent);
        AgentRunner.startOnThread(clientAgentRunner);

        await().until(() -> clientAgent.isActive());
    }

    public TestOrder placeOrderAndAwaitResult(final double price, final long size, final Side side)
    {
        final long correlationId = clientAgent.placeOrder(price, size, side);
        final ExecutionResult executionResult = clientAgent.getReceivedExecutionResult(correlationId);
        return new TestOrder(executionResult.getOrderId(), price, size);
    }

    public Status clearOrderbook()
    {
        return clientAgent.getReceivedStatus(clientAgent.sendHeaderMessage(Method.CLEAR));
    }

    public long requestCurrentIdAndAwaitCurrentOrderId()
    {
        return clientAgent.getReceivedOrderId(clientAgent.sendHeaderMessage(Method.CURRENT_ORDER_ID));
    }

    public ExecutionResult requestCancelOrderAndAwaitResult(final long orderId)
    {
        return clientAgent.getReceivedExecutionResult(clientAgent.cancelOrder(orderId));
    }

    public Status resetOrderbook()
    {
        return clientAgent.getReceivedStatus(clientAgent.sendHeaderMessage(Method.RESET));
    }

    public List<Order> requestAndReceiveAllAsks()
    {
        return clientAgent.getReceivedOrders(clientAgent.sendHeaderMessage(Method.ASKS));
    }

    public List<Order> requestAndReceiveAllBids()
    {
        return clientAgent.getReceivedOrders(clientAgent.sendHeaderMessage(Method.BIDS));
    }

    @Override
    public void close()
    {
        CloseHelper.quietCloseAll(clientAgentRunner);
    }
}
