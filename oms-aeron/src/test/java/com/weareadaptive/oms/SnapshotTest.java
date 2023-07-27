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
            JsonObject orderAsk = new JsonObject();
            orderAsk.put("method", "place");
            OrderCommand askOrder = new OrderCommand(100,1, Side.ASK);
            orderAsk.put("order", JsonObject.mapFrom(askOrder));
            Buffer request1 = Buffer.buffer(orderAsk.encode());
            websocket.write(request1);
            websocket.write(request1);

            JsonObject orderBid = new JsonObject();
            orderBid.put("method", "place");
            OrderCommand order = new OrderCommand(1, 100, Side.BID);
            orderBid.put("order", JsonObject.mapFrom(order));
            Buffer request2 = Buffer.buffer(orderBid.encode());
            websocket.write(request2);
            websocket.write(request2);
        });

        // todo: make sure the snapshot that is written can then be read
        try {
            File outputFile = new File("snapshot.dat");
            PrintStream filePrintStream = new PrintStream(new FileOutputStream(outputFile));
            deployment.getNodes().forEach((id, node) -> ClusterTool.snapshot(node.getClusterDir(), filePrintStream));
        } catch (FileNotFoundException e) {
            LOGGER.error(e.toString());
        }

        deployment.shutdownGateway();
        deployment.shutdownCluster();

        deployment.startSingleNodeCluster(false);
        // todo: check if state is the same
    }
}
