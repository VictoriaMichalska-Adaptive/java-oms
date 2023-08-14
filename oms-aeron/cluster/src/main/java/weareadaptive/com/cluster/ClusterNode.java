package weareadaptive.com.cluster;

import io.aeron.CommonContext;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static weareadaptive.com.cluster.util.ConfigUtils.*;

public class ClusterNode
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterNode.class);

    ClusteredServiceContainer clusteredServiceContainer;
    ClusteredMediaDriver clusteredMediaDriver;
    ClusterService clusterService = new ClusterService();
    File clusterDir;
    boolean active = false;

    ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

    public void startNode(final int node, final int maxNodes, final boolean test)
    {
        LOGGER.info("Starting Cluster Node...");
        barrier = new ShutdownSignalBarrier();


        final int portBase = getBasePort();
        final String hosts = maxNodes == 1 ? getClusterAddresses() : getMultiNodeClusterAddresses(maxNodes);

        //This may need tuning for your environment.
        final File baseDir =
            new File(System.getProperty("user.dir") + "/oms-aeron/complete" + "/aeronCluster/", "node" + node);
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + node + "-driver";

        LOGGER.info("Base Dir: " + baseDir);
        LOGGER.info("Aeron Dir: " + aeronDirName);

        final List<String> hostAddresses = List.of(hosts.split(","));
        final ClusterConfig clusterConfig = ClusterConfig.create(
                node,
            hostAddresses,
            hostAddresses,
            portBase,
            clusterService
        );
        clusterConfig.consensusModuleContext().ingressChannel("aeron:udp");

        clusterConfig.archiveContext().archiveDir(new File(baseDir, "archive"));
        clusterDir = new File(baseDir, "cluster");
        clusterConfig.clusteredServiceContext().clusterDir(clusterDir);
        clusterConfig.consensusModuleContext().clusterDir(clusterDir);
        clusterConfig.consensusModuleContext().deleteDirOnStart(test);
        clusterConfig.mediaDriverContext().aeronDirectoryName(aeronDirName);
        clusterConfig.archiveContext().aeronDirectoryName(aeronDirName);
        clusterConfig.aeronArchiveContext().aeronDirectoryName(aeronDirName);
        clusterConfig.consensusModuleContext().egressChannel(egressChannel());
        //This may need tuning for your environment.
        clusterConfig.consensusModuleContext().leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1));

        awaitDnsResolution(hostAddresses, node);

        try (
            ClusteredMediaDriver clusteredMediaDriver = ClusteredMediaDriver.launch(
                clusterConfig.mediaDriverContext(),
                clusterConfig.archiveContext(),
                clusterConfig.consensusModuleContext());
            ClusteredServiceContainer clusteredServiceContainer = ClusteredServiceContainer.launch(
                clusterConfig.clusteredServiceContext()))
        {
            this.clusteredServiceContainer = clusteredServiceContainer;
            this.clusteredMediaDriver = clusteredMediaDriver;
            LOGGER.info("Started Cluster Node...");
            setActive(true);

            barrier.await();

            CloseHelper.quietClose(clusteredMediaDriver);
            CloseHelper.quietClose(clusteredServiceContainer);

            setActive(false);
            LOGGER.info("Exiting");
        }
    }

    public boolean isActive()
    {
        return this.active;
    }

    public void setActive(final boolean active)
    {
        this.active = active;
    }

    public ShutdownSignalBarrier getBarrier()
    {
        return barrier;
    }

    public int getLeaderId()
    {
        return clusterService.getCurrentLeader();
    }

    public File getClusterDir() {
        return clusterDir;
    }
}
