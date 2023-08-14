package weareadaptive.com.gateway.util;

import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.concurrent.SystemEpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.parseInt;

public class ConfigUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * Read the cluster addresses from the environment variable CLUSTER_ADDRESSES or the
     * system property cluster.addresses
     *
     * @return cluster addresses
     */
    public static String getClusterAddresses()
    {
        String clusterAddresses = System.getenv("CLUSTER_ADDRESSES");
        if (null == clusterAddresses || clusterAddresses.isEmpty())
        {
            clusterAddresses = System.getProperty("cluster.addresses", "localhost");
        }
        return clusterAddresses;
    }

    public static String getMultiNodeClusterAddresses(final int maxNodes)
    {
        String clusterAddresses = System.getenv("CLUSTER_ADDRESSES");
        if (null == clusterAddresses || clusterAddresses.isEmpty())
        {
            clusterAddresses = System.getProperty("cluster.addresses", "localhost");
        }

        if (clusterAddresses.equalsIgnoreCase("localhost"))
        {
            ArrayList<String> allAddresses = new ArrayList<>();
            for (int i = 0; i < maxNodes; i++)
            {
                allAddresses.add(clusterAddresses);
            }
            return String.join(",", allAddresses);
        }

        return clusterAddresses;
    }

    /**
     * Get the cluster node id
     *
     * @return cluster node id, default 0
     */
    public static int getClusterNode()
    {
        String clusterNode = System.getenv("CLUSTER_NODE");
        if (null == clusterNode || clusterNode.isEmpty())
        {
            clusterNode = System.getProperty("node.id", "0");
        }
        return parseInt(clusterNode);
    }

    /**
     * Get the base port for the cluster configuration
     *
     * @return base port, default 9000
     */
    public static int getBasePort()
    {
        String portBaseString = System.getenv("CLUSTER_PORT_BASE");
        if (null == portBaseString || portBaseString.isEmpty())
        {
            portBaseString = System.getProperty("port.base", "9000");
        }
        return parseInt(portBaseString);
    }

    /**
     * Await DNS resolution of self. Under Kubernetes, this can take a while.
     *
     * @param hostArray host array
     * @param nodeId    node id
     */
    public static void awaitDnsResolution(final List<String> hostArray, final int nodeId)
    {
        if (applyDnsDelay())
        {
            LOGGER.info("Waiting 5 seconds for DNS to be registered...");
            quietSleep(5000);
        }

        final long endTime = SystemEpochClock.INSTANCE.time() + 60000;
        final String nodeName = hostArray.get(nodeId);
        java.security.Security.setProperty("networkaddress.cache.ttl", "0");

        boolean resolved = false;
        while (!resolved)
        {
            if (SystemEpochClock.INSTANCE.time() > endTime)
            {
                LOGGER.error("cannot resolve name {}, exiting", nodeName);
                System.exit(-1);
            }

            try
            {
                InetAddress.getByName(nodeName);
                resolved = true;
            } catch (final UnknownHostException e)
            {
                LOGGER.warn("cannot yet resolve name {}, retrying in 3 seconds", nodeName);
                quietSleep(3000);
            }
        }
    }

    /**
     * Sleeps for the given number of milliseconds, ignoring any interrupts.
     *
     * @param millis the number of milliseconds to sleep.
     */
    private static void quietSleep(final long millis)
    {
        try
        {
            Thread.sleep(millis);
        } catch (final InterruptedException ex)
        {
            LOGGER.warn("Interrupted while sleeping");
        }
    }

    /**
     * Apply DNS delay
     *
     * @return true if DNS delay should be applied
     */
    private static boolean applyDnsDelay()
    {
        final String dnsDelay = System.getenv("DNS_DELAY");
        if (null == dnsDelay || dnsDelay.isEmpty())
        {
            return false;
        }
        return Boolean.parseBoolean(dnsDelay);
    }

    /**
     * Read the cluster addresses, port and port offsets to return ingress endpoints
     *
     * @return Ingress endpoints
     */
    public static String ingressEndpoints(final int maxNodes)
    {
        return ClusterConfig.ingressEndpoints(
            Arrays.asList(
                maxNodes != 1 ? getMultiNodeClusterAddresses(maxNodes).split(",") : getClusterAddresses().split(",")),
            getBasePort(),
            ClusterConfig.CLIENT_FACING_PORT_OFFSET
        );
    }

    public static String egressChannel()
    {
        String clusterAddresses = System.getenv("EGRESS_CHANNEL");
        if (null == clusterAddresses || clusterAddresses.isEmpty())
        {
            clusterAddresses = System.getProperty("EGRESS_CHANNEL", "aeron:udp?endpoint=localhost:0");
        }
        return clusterAddresses;
    }
}
