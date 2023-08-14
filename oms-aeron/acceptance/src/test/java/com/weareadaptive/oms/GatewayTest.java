package com.weareadaptive.oms;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GatewayTest
{
    Deployment deployment;

    @BeforeEach
    void startup() {
        deployment = new Deployment();
    }

    @AfterEach
    void teardown()
    {
        deployment.shutdownCluster();
        deployment.shutdownGateway();
    }

    @Test
    @DisplayName("A gateway is launched and connects to the cluster")
    void gatewayConnects() throws InterruptedException {
        deployment.startCluster(3, true);
        deployment.getNodes().forEach((id, node) -> assertTrue(node.isActive()));
        deployment.startGateway(3);
        assertEquals(deployment.getGateway().getLeaderId(), deployment.getLeaderId());
    }

}
