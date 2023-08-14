package weareadaptive.com.gateway.client;

import io.aeron.cluster.client.AeronCluster;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientIngressSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientIngressSender.class);
    AeronCluster aeronCluster;

    public ClientIngressSender(final AeronCluster aeronCluster)
    {
        this.aeronCluster = aeronCluster;
    }

    public void sendMessageToCluster(MutableDirectBuffer buffer, int length) {
        while (aeronCluster.offer(buffer, 0, length) < 0);
    }
}
