package com.weareadaptive.oms;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterTest
{
    Deployment deployment;

    @BeforeEach
    void startup() {
        deployment = new Deployment();
    }

    @AfterEach
    void teardown() {
        deployment.shutdownCluster();
    }

    @Test
    @DisplayName("A single node cluster has started")
    void singleNodeClusterStart() throws InterruptedException {
        deployment.startSingleNodeCluster(true);
        deployment.getNodes().forEach((id, node) -> assertTrue(node.isActive()));
    }

    @Test
    @DisplayName("A single node cluster has started and gracefully shutdown")
    void singleNodeClusterStartThenStop() throws InterruptedException {
        deployment.startSingleNodeCluster(true);
        deployment.getNodes().forEach((id, node) -> assertTrue(node.isActive()));
        deployment.shutdownCluster();
        deployment.getNodes().forEach((id, node) -> assertFalse(node.isActive()));
    }

    @Test
    @DisplayName("A cluster of 3 nodes has started")
    void clusterStart() throws InterruptedException {
        deployment.startCluster(3, true);
        deployment.getNodes().forEach((id, node) -> assertTrue(node.isActive()));
    }

    @Test
    @DisplayName("A cluster of 3 nodes has started and gracefully shutdown")
    void clusterStartThenStop() throws InterruptedException {
        deployment.startCluster(3, true);
        deployment.getNodes().forEach((id, node) -> assertTrue(node.isActive()));
        deployment.shutdownCluster();
        deployment.getNodes().forEach((id, node) -> assertFalse(node.isActive()));
    }

    @Test
    @DisplayName("A leadership election results in a new node being elected")
    void clusterLeadershipElection() throws InterruptedException {
        clusterStart();
        boolean newLeader = deployment.initiateLeadershipElection();
        assertTrue(newLeader);
    }
}
