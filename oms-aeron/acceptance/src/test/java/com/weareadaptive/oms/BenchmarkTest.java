package com.weareadaptive.oms;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import weareadaptive.com.cluster.ClusterNode;
import weareadaptive.com.cluster.services.oms.util.Side;
import weareadaptive.com.gateway.ws.command.OrderCommand;

import java.util.concurrent.TimeUnit;

import static io.vertx.core.Vertx.vertx;
import static org.awaitility.Awaitility.await;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class BenchmarkTest
{
    private final Deployment deployment = new Deployment();
    private final Vertx vertx = vertx();
    private HttpClient vertxClient;
    JsonObject orderRequest = new JsonObject();
    OrderCommand order = new OrderCommand(10, 15, Side.ASK);
    private final Buffer request;

    // todo: random orders should be placed
    public BenchmarkTest()
    {
        orderRequest.put("method", "place");
        orderRequest.put("order", JsonObject.mapFrom(order));
        request = Buffer.buffer(orderRequest.encode());
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Setup(Level.Trial)
    public void setup() throws InterruptedException
    {
        deployment.startCluster(1, true);
        await().until(() -> deployment.getNodes().values().stream().anyMatch(ClusterNode::isActive));
        deployment.startGateway(1);
        await().until(() -> deployment.getGateway().isActive());
        vertxClient = vertx.createHttpClient();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        deployment.shutdownCluster();
        await().until(() -> deployment.getNodes().values().stream().noneMatch(ClusterNode::isActive));
        deployment.shutdownGateway();
        await().until(() -> !deployment.getGateway().isActive());
        vertxClient.close();
        vertx.close();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void hundredOrders(final Blackhole blackhole) {
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            // todo: figure out how to actually interate in benchmark
            for (int i = 0; i < 100; i++)
            {
                websocket.write(request);

                websocket.handler(response -> {
                    System.out.println(response);
                    blackhole.consume(response);
                });
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void thousandOrders(final Blackhole blackhole) {
        vertxClient.webSocket(8080, "localhost", "/").onSuccess(websocket -> {
            for (int i = 0; i < 1000; i++)
            {
                websocket.write(request);

                websocket.handler(response -> {
                    System.out.println(response);
                    blackhole.consume(response);
                });
            }
        });
    }
}
