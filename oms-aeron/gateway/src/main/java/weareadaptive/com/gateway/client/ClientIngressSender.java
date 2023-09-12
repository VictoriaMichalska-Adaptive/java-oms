package weareadaptive.com.gateway.client;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weareadaptive.com.cluster.services.oms.util.Method;
import weareadaptive.com.cluster.services.oms.util.Side;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientIngressSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientIngressSender.class);
    final private AeronCluster aeronCluster;
    final private ConcurrentLinkedQueue<DirectBuffer> bufferQueue = new ConcurrentLinkedQueue<>();
    final private Queue<Integer> lengthQueue = new LinkedList<>();
    final private Encoder encoder = new Encoder();

    public ClientIngressSender(final AeronCluster aeronCluster)
    {
        this.aeronCluster = aeronCluster;
    }

    public void sendMessageToCluster(final DirectBuffer buffer, final int length) {
        long offerResponse = 0;

        bufferQueue.add(buffer);
        lengthQueue.add(length);
        while (!bufferQueue.isEmpty() && !lengthQueue.isEmpty()) {
            offerResponse = aeronCluster.offer(bufferQueue.peek(), 0, lengthQueue.peek());
            if (offerResponse < 0) break;
            else {
                bufferQueue.remove();
                lengthQueue.remove();
            }
        }

        if (offerResponse == (int) Publication.MAX_POSITION_EXCEEDED ||
                offerResponse == (int) Publication.CLOSED ||
                offerResponse == (int) Publication.NOT_CONNECTED)
            {
                bufferQueue.clear();
                lengthQueue.clear();
            }
    }

    public void sendOrderRequestToCluster(final long correlationId, final double price, final long size, final Side side)
    {
        LOGGER.info("OrderRequest is being sent to cluster");
        sendMessageToCluster(encoder.encodeOrderRequest(correlationId, price, size, side), encoder.ORDER_REQUEST_LENGTH);
    }

    public void sendCancelOrderToCluster(final long correlationId, final long orderId)
    {
        LOGGER.info("CancelOrder is being sent to cluster");
        sendMessageToCluster(encoder.encodeCancelOrder(correlationId, orderId), encoder.CANCEL_ORDER_LENGTH);
    }

    public void sendHeaderMessageToCluster(long correlationId, Method method)
    {
        LOGGER.info("HeaderMessage is being sent to cluster");
        sendMessageToCluster(encoder.encodeHeaderMessage(correlationId, method), encoder.HEADER_LENGTH);
    }
}
