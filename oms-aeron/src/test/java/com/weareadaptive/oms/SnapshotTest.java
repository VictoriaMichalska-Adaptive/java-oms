package com.weareadaptive.oms;

import com.weareadaptive.cluster.services.oms.util.Side;
import com.weareadaptive.gateway.ws.command.OrderCommand;
import io.aeron.cluster.ClusterTool;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import static io.vertx.core.Vertx.vertx;

@ExtendWith(VertxExtension.class)
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
        deployment.startSingleNodeCluster(true);
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
    public void clusterCanRecoverToStateFromSnapshot(final VertxTestContext testContext) throws Throwable
    {
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            JsonObject orderRequest = new JsonObject();
            orderRequest.put("method", "place");
            OrderCommand order = new OrderCommand(10, 15, Side.ASK);
            orderRequest.put("order", JsonObject.mapFrom(order));
            Buffer request = Buffer.buffer(orderRequest.encode());
            websocket.write(request);
        });

        try {
            File outputFile = new File("output.txt");
            PrintStream filePrintStream = new PrintStream(new FileOutputStream(outputFile));
            deployment.getNodes().forEach((id, node) -> ClusterTool.snapshot(node.getClusterDir(), filePrintStream));
        } catch (FileNotFoundException e) {
            LOGGER.error(e.toString());
        }
        deployment.shutdownGateway();
        deployment.shutdownCluster();

        deployment.startSingleNodeCluster(false);
    }
}
