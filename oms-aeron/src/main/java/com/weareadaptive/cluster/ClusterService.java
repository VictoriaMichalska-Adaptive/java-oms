package com.weareadaptive.cluster;

import com.weareadaptive.cluster.services.OMSService;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.weareadaptive.util.Decoder.HEADER_SIZE;
import static com.weareadaptive.util.Decoder.decodeOMSHeader;

public class ClusterService implements ClusteredService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusteredService.class);
    private OMSService omsService;
    private int currentLeader = -1;

    /**
     * * Logic executed on cluster start
     * - Register services containing business logic and state
     * - Restore state in business logic using snapshot
     */
    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        registerOMSService();
        restoreSnapshot(snapshotImage);
    }

    /**
     * * When a cluster client has connected to the cluster
     */
    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        LOGGER.info("Client ID: " + session.id() + " Connected");
    }

    /**
     * * When a cluster client has disconnected to the cluster
     */
    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        LOGGER.info("Client ID: " + session.id() + " Disconnected");
    }

    /**
     * * When the cluster has received Ingress from a cluster client
     */
    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                 final int offset, final int length,
                                 final Header header)
    {
        LOGGER.info("Client ID: " + session.id() + " Ingress");

        final var customHeader = decodeOMSHeader(buffer, offset);
        switch (customHeader.getServiceName())
        {
            case OMS -> omsService.messageHandler(session, customHeader, buffer, offset + HEADER_SIZE);
            case NONE -> LOGGER.error("Bad service name");
        }
    }

    /**
     * * Orderbook state should be snapshotted for restoring state on cluster start.
     * - Convert data into binary encodings
     * - Offer to snapshotPublication
     */
    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
    }

    /**
     * * Orderbook state should be restored from snapshotImage on cluster start.
     * - Convert binary encodings into data
     * - Use data to restore Orderbook state
     */
    public void restoreSnapshot(final Image snapshotImage)
    {
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp)
    {

    }

    /**
     * * When the cluster node changes role on election
     */
    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
        LOGGER.info("Cluster node is now: " + newRole);
    }

    /**
     * * When the cluster node is terminating
     */
    @Override
    public void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        LOGGER.info("Cluster node " + leaderMemberId + " is now Leader, previous Leader: " + currentLeader);
        currentLeader = leaderMemberId;
    }

    /**
     * * When the cluster node is terminating
     */
    @Override
    public void onTerminate(final Cluster cluster)
    {
        LOGGER.info("Cluster node is terminating");
    }

    private void registerOMSService()
    {
        omsService = new OMSService();
    }

    public int getCurrentLeader()
    {
        return this.currentLeader;
    }
}
