package weareadaptive.com.gateway.ws.command;

import weareadaptive.com.cluster.services.oms.util.Side;

public record OrderCommand(double price, long size, Side side)
{
}
