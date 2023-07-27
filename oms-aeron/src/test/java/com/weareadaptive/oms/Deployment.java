package com.weareadaptive.oms;

import com.weareadaptive.cluster.ClusterNode;
import com.weareadaptive.gateway.Gateway;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Deployment {

    private final HashMap<Integer, ClusterNode> nodes = new HashMap<>();
    private final HashMap<Integer, Thread> nodeThreads = new HashMap<>();
    private Gateway gateway;
    private Thread gatewayThread;
    public Gateway getGateway() {
        return gateway;
    }

    public Thread getGatewayThread() {
        return gatewayThread;
    }

    public HashMap<Integer, ClusterNode> getNodes() {
        return nodes;
    }

    public HashMap<Integer, Thread> getNodeThreads() {
        return nodeThreads;
    }

    public void startSingleNodeCluster(boolean testing) throws InterruptedException {
        startNode(0,1, testing);
    }

    public void startCluster() throws InterruptedException {
        startNode(0,3, true);
        startNode(1,3, true);
        startNode(2,3, true);
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

    void startGateway() throws InterruptedException {
        gateway = new Gateway();

        gatewayThread = new Thread(() ->
                gateway.startGateway(3)
        );

        gatewayThread.start();
        gatewayThread.join(2500);
    }

    void startGateway(Handler<AsyncResult<String>> testContext) throws InterruptedException {
        gateway = new Gateway(testContext);

        gatewayThread = new Thread(() ->
                gateway.startGateway(3)
        );

        gatewayThread.start();
        gatewayThread.join(2500);
    }

    void shutdownGateway() throws InterruptedException {
        gateway.shutdown();
        gatewayThread.interrupt();
        gatewayThread.join();
    }
}
