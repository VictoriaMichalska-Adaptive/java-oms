package com.weareadaptive.gateway;

import com.weareadaptive.gateway.client.ClientEgressListener;
import com.weareadaptive.gateway.client.ClientIngressSender;
import com.weareadaptive.gateway.ws.WebSocketServer;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.vertx.core.Vertx;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.weareadaptive.util.ConfigUtils.egressChannel;
import static com.weareadaptive.util.ConfigUtils.ingressEndpoints;


public class Gateway
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Gateway.class);
    private static final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
    private static ClientEgressListener clientEgressListener;
    private static ClientIngressSender clientIngressSender;
    private static WebSocketServer webSocketServer;
    private static Vertx vertx;
    private static Thread keepClusterAlive;
    private static boolean isActive;
    private int initLeader = -1;

    private static void startWSServer()
    {
        vertx = Vertx.vertx();
        webSocketServer = new WebSocketServer(clientIngressSender, clientEgressListener);
        vertx.deployVerticle(webSocketServer);
        LOGGER.info("Websocket started...");
    }

    public void startGateway(final int maxNodes)
    {
        LOGGER.info("Starting Gateway...");
        clientEgressListener = new ClientEgressListener();

        MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .threadingMode(ThreadingMode.DEDICATED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
        );
        try (
            AeronCluster aeronCluster = AeronCluster.connect(
                new AeronCluster.Context()
                    .egressListener(clientEgressListener)
                    .egressChannel(egressChannel())
                    .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                    .ingressChannel("aeron:udp")
                    .ingressEndpoints(ingressEndpoints(maxNodes))
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                    .errorHandler(new ErrorHandler()
                    {
                        @Override
                        public void onError(final Throwable throwable)
                        {
                            System.err.println(throwable);
                        }
                    })
            )
        )
        {
            clientIngressSender = new ClientIngressSender(aeronCluster);
            LOGGER.info("Aeron Client started...");
            startWSServer();

            isActive = true;
            initLeader = aeronCluster.leaderMemberId();

            /**
             * * A heartbeat loop to ensure the cluster client stays open
             *  - Without this, the client will close if it doesn't:
             *      - Send ingress
             *      - Receive egress
             */
            keepClusterAlive = new Thread(() ->
            {
                while (true)
                {
                    aeronCluster.sendKeepAlive();
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                }
            }
            );
            keepClusterAlive.start();

            while (isActive)
            {
                aeronCluster.pollEgress();
                idleStrategy.idle();
            }
        }
    }

    public int getLeaderId()
    {
        return clientEgressListener.getCurrentLeader() == -1 ? initLeader : clientEgressListener.getCurrentLeader();
    }

    public void shutdown()
    {
        keepClusterAlive.interrupt();
        isActive = false;
        vertx.close();
    }
}
