package com.weareadaptive.oms;

import com.weareadaptive.oms.dsl.TestClientDsl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.agrona.CloseHelper;
import weareadaptive.com.cluster.ClusterNode;
import weareadaptive.com.gateway.Gateway;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Deployment implements AutoCloseable
{
    private final HashMap<Integer, ClusterNode> nodes = new HashMap<>();
    private final HashMap<Integer, Thread> nodeThreads = new HashMap<>();
    private Gateway gateway;
    public Gateway getGateway() {
        return gateway;
    }
    private TestClientDsl testClientDsl;

    public TestClientDsl getTestClientDsl()
    {
        return testClientDsl;
    }

    public HashMap<Integer, ClusterNode> getNodes() {
        return nodes;
    }

    public HashMap<Integer, Thread> getNodeThreads() {
        return nodeThreads;
    }

    public void startTestClientDsl(final int maxNodes) {
        testClientDsl = new TestClientDsl(maxNodes);
        testClientDsl.startClient();
    }

    public void startSingleNodeCluster(boolean testing) throws InterruptedException {
        startNode(0,1, testing);
    }

    public void startCluster(final int maxNodes, boolean testing) throws InterruptedException {
        for (int i = 0; i < maxNodes; i++)
        {
            startNode(i, maxNodes, testing);
        }
    }

    public void shutdownCluster() {
        nodes.forEach( (id, node) -> {
            if (node != null && node.isActive()) {
                // Signal the node to shutdown
                node.getBarrier().signal();
                // Wait for the node to shut down
                waitToDie(node);
            }
        });

        nodeThreads.forEach((id, thread) -> {
            if (thread != null) {
                // Interrupt the thread if it's still running
                thread.interrupt();
                // Join the thread
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public void startNode(int nodeId, int maxNodes, boolean testing) throws InterruptedException {
        ClusterNode node = new ClusterNode();
        nodes.put(nodeId,node);

        Thread nodeThread = new Thread(() ->
                node.startNode(nodeId, maxNodes, testing)
        );

        nodeThreads.put(
                nodeId,
                nodeThread
        );
        nodeThread.start();
        waitToStart(node);
        nodeThread.join(2500);
    }

    public void stopNode(int nodeId) throws InterruptedException {
        ClusterNode node = nodes.get(nodeId);
        node.getBarrier().signal();
        waitToDie(node);
        nodeThreads.get(nodeId).interrupt();
        nodeThreads.get(nodeId).join();
        nodes.remove(nodeId);
        nodeThreads.remove(nodeId);
    }

    public void waitToStart(ClusterNode node) {
        final int TIMEOUT_LIMIT = 50;
        int TIMEOUT_COUNTER = 0;
        while(!node.isActive())  {
            if (TIMEOUT_COUNTER == TIMEOUT_LIMIT) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            TIMEOUT_COUNTER++;
        }
        assertTrue(node.isActive());
    }

    public void waitToDie(ClusterNode node) {
        final int TIMEOUT_LIMIT = 50;
        int TIMEOUT_COUNTER = 0;
        while(node.isActive())  {
            if (TIMEOUT_COUNTER == TIMEOUT_LIMIT) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            TIMEOUT_COUNTER++;
        }
        assertFalse(node.isActive());
    }

    public void waitForElection(ClusterNode node) {
        final int TIMEOUT_LIMIT = 500;
        int TIMEOUT_COUNTER = 0;
        while(node.getLeaderId() == -1)  {
            if (TIMEOUT_COUNTER == TIMEOUT_LIMIT) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            TIMEOUT_COUNTER++;
        }
    }

    public int findAliveNode() {
        return nodes.keySet().stream().findFirst().get();
    }

    public int getLeaderId() {
        return nodes.get(findAliveNode()).getLeaderId();
    }

    public boolean initiateLeadershipElection() throws InterruptedException {
        waitForElection(nodes.get(findAliveNode()));
        int startingLeader = getLeaderId();
        stopNode(startingLeader);
        nodeThreads.get(findAliveNode()).join(2500);
        int newLeader = getLeaderId();
        return startingLeader != newLeader;
    }

    void startGateway(final int maxNodes)
    {
        gateway = new Gateway(maxNodes);
        gateway.startGateway();
    }

    void startGateway(final int maxNodes, Handler<AsyncResult<String>> testContext)
    {
        gateway = new Gateway(maxNodes, testContext);
        gateway.startGateway();
    }

    void shutdownGateway()
    {
        CloseHelper.close(gateway);
    }

    @Override
    public void close()
    {
        CloseHelper.closeAll(gateway);
        CloseHelper.closeAll(testClientDsl);
        CloseHelper.closeAll(nodes.values());
    }
}
