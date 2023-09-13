package com.weareadaptive.oms.dsl;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.SystemEpochClock;
import weareadaptive.com.cluster.services.oms.util.*;
import weareadaptive.com.gateway.client.ClientIngressSender;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static weareadaptive.com.gateway.util.ConfigUtils.egressChannel;
import static weareadaptive.com.gateway.util.ConfigUtils.ingressEndpoints;

public class TestGatewayAgent implements Agent
{
    private final int maxNodes;
    private TestClientEgressListener clientEgressListener;
    private ClientIngressSender clientIngressSender;
    private AeronCluster aeronCluster;
    private MediaDriver mediaDriver;
    private long lastHeartbeatTime = Long.MIN_VALUE;
    private static final long HEARTBEAT_INTERVAL = 250;
    private boolean isActive;
    private long currentCorrelationId = 0;

    public TestGatewayAgent(final int maxNodes)
    {
        this.maxNodes = maxNodes;
    }

    @Override
    public void onStart()
    {
        clientEgressListener = new TestClientEgressListener();

        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        aeronCluster = AeronCluster.connect(
                new AeronCluster.Context()
                        .egressListener(clientEgressListener)
                        .egressChannel(egressChannel())
                        .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                        .ingressChannel("aeron:udp")
                        .ingressEndpoints(ingressEndpoints(maxNodes))
                        .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                        .errorHandler(null));

        clientIngressSender = new ClientIngressSender(aeronCluster);
        isActive = true;
    }

    @Override
    public int doWork()
    {
        final long now = SystemEpochClock.INSTANCE.time();
        if (now >= (lastHeartbeatTime + HEARTBEAT_INTERVAL))
        {
            lastHeartbeatTime = now;
            if (isActive)
            {
                aeronCluster.sendKeepAlive();
            }
        }
        if (null != aeronCluster && !aeronCluster.isClosed())
        {
            return aeronCluster.pollEgress();
        }
        return 0;
    }

    @Override
    public void onClose()
    {
        if (aeronCluster != null)
        {
            aeronCluster.close();
        }
        if (mediaDriver != null)
        {
            mediaDriver.close();
        }
    }

    @Override
    public String roleName()
    {
        return "test-gateway-agent";
    }

    protected long placeOrder(final double price, final long size, final Side side)
    {
        currentCorrelationId += 1;
        clientIngressSender.sendOrderRequestToCluster(currentCorrelationId, price, size, side);
        return currentCorrelationId;
    }

    protected ExecutionResult getReceivedExecutionResult(final long correlationId)
    {
        await().until(() -> clientEgressListener.receivedExecutionResult(correlationId));

        return clientEgressListener.getReceivedExecutionResult(correlationId);
    }

    protected long cancelOrder(final long orderId)
    {
        currentCorrelationId += 1;
        clientIngressSender.sendCancelOrderToCluster(currentCorrelationId, orderId);
        return currentCorrelationId;
    }

    protected long sendHeaderMessage(final Method method)
    {
        currentCorrelationId += 1;
        clientIngressSender.sendHeaderMessageToCluster(currentCorrelationId, method);
        return currentCorrelationId;
    }

    protected Status getReceivedStatus(final long correlationId)
    {
        await().until(() -> clientEgressListener.receivedStatus(correlationId));

        return clientEgressListener.getReceivedStatus(correlationId);
    }

    protected long getReceivedOrderId(final long correlationId)
    {
        await().until(() -> clientEgressListener.receivedOrderId(correlationId));

        return clientEgressListener.getReceivedOrderId(correlationId);
    }

    protected List<Order> getReceivedOrders(final long correlationId)
    {
        await().until(() -> clientEgressListener.receivedOrders(correlationId));

        return clientEgressListener.getReceivedOrders(correlationId);
    }

    protected boolean isActive()
    {
        return isActive;
    }
}
