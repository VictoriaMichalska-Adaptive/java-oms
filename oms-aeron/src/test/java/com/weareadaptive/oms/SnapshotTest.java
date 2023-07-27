package com.weareadaptive.oms;

import io.aeron.Aeron;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.service.ClusteredService;
import io.vertx.core.http.HttpClient;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import static io.vertx.core.Vertx.vertx;

public class SnapshotTest
{
    // todo: MAKE TESTS TO SEE IF SNAPSHOTTING WORKS
    private Deployment deployment;
    private HttpClient vertxClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotTest.class);

    @BeforeEach
    void setUp(final VertxTestContext testContext) throws InterruptedException
    {
        deployment = new Deployment();
        deployment.startCluster();
        deployment.startGateway(testContext.succeeding(id -> testContext.completeNow()));
        vertxClient = vertx().createHttpClient();
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        vertxClient.close();
        deployment.shutdownGateway();
        deployment.shutdownCluster();
    }

    @Test
    public void clusterCanRecoverToStateFromSnapshot() throws InterruptedException
    {
        deployment.startSingleNodeCluster();

        // todo: give the node a certain non default state

        try {
            File outputFile = new File("output.txt");
            PrintStream filePrintStream = new PrintStream(new FileOutputStream(outputFile));
            deployment.getNodes().forEach((id, node) -> ClusterTool.snapshot(node.getClusterDir(), filePrintStream));
        } catch (FileNotFoundException e) {
            LOGGER.error(e.toString());
        }

        deployment.startSingleNodeCluster();
    }
}
