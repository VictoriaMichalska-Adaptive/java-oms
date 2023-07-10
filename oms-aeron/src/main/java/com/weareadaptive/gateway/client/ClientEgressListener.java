package com.weareadaptive.gateway.client;

import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientEgressListener implements EgressListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientEgressListener.class);
    private int currentLeader = -1;

    /**
     * * Implement Egress routing logic
     */
    @Override
    public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                          final int offset, final int length,
                          final Header header)
    {
        //Decode buffer to receive 1 int echo from cluster
        LOGGER.info("Cluster Egress: " + buffer.getInt(offset));
    }

    @Override
    public void onSessionEvent(final long correlationId, final long clusterSessionId, final long leadershipTermId,
                               final int leaderMemberId,
                               final EventCode code, final String detail)
    {
        EgressListener.super.onSessionEvent(correlationId, clusterSessionId, leadershipTermId, leaderMemberId, code,
            detail);
    }

    @Override
    public void onNewLeader(final long clusterSessionId, final long leadershipTermId, final int leaderMemberId,
                            final String ingressEndpoints)
    {
        LOGGER.info("Cluster node " + leaderMemberId + " is now Leader, previous Leader: " + currentLeader);
        currentLeader = leaderMemberId;
    }

    public int getCurrentLeader()
    {
        return this.currentLeader;
    }

}
