package weareadaptive.com.gateway;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.agrona.CloseHelper;
import org.agrona.concurrent.*;

public class Gateway implements AutoCloseable
{
    private static GatewayAgent clientAgent;
    private static AgentRunner clientAgentRunner;
    private static IdleStrategy idleStrategy = new BusySpinIdleStrategy();
    private final int maxNodes;
    private Handler<AsyncResult<String>> testContext;

    public Gateway(final int maxNodes, final Handler<AsyncResult<String>> testContext)
    {
        this.maxNodes = maxNodes;
        this.testContext = testContext;
    }
    public Gateway(final int maxNodes) {
        this.maxNodes = maxNodes;
    }

    public void startGateway()
    {
        if (testContext == null) {
            clientAgent = new GatewayAgent(maxNodes);
        }
        else {
            clientAgent = new GatewayAgent(maxNodes, testContext);
        }
        clientAgentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace,
                null, clientAgent);
        AgentRunner.startOnThread(clientAgentRunner);
    }

    public int getLeaderId()
    {
        return clientAgent.getLeaderId();
    }

    public static void main(String[] args)
    {
        final int maxNodes = args.length > 0 ? Integer.parseInt(args[0]) : 1;

        idleStrategy = new SleepingMillisIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        clientAgent = new GatewayAgent(maxNodes);
        clientAgentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace,
                null, clientAgent);
        AgentRunner.startOnThread(clientAgentRunner);

        barrier.await();

        CloseHelper.quietCloseAll(clientAgentRunner);
    }

    @Override
    public void close()
    {
        CloseHelper.quietCloseAll(clientAgentRunner);
    }

    public boolean isActive() {
        return !clientAgentRunner.isClosed();
    }
}
