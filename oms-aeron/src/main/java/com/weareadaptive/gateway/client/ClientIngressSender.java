package com.weareadaptive.gateway.client;

import io.aeron.cluster.client.AeronCluster;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ClientIngressSender
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientIngressSender.class);
    AeronCluster aeronCluster;

    public ClientIngressSender(final AeronCluster aeronCluster)
    {
        this.aeronCluster = aeronCluster;

        //Offer a simple 0 Echo to the cluster on client start
        OfferEcho();
    }

    private void OfferEcho()
    {
        //Encode 0 int into a buffer
        final MutableDirectBuffer msgBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Integer.BYTES));
        msgBuffer.putInt(0, 0);

        //Offer buffer with offset and buffer length
        while (aeronCluster.offer(msgBuffer, 0, Integer.BYTES) < 0) ;
    }
}
