package weareadaptive.com.gateway.ws.command;

import weareadaptive.com.cluster.services.oms.util.Status;

public record ExecutionResultCommand(Status status, long orderId)
{
}
