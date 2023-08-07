package com.weareadaptive.gateway.ws.command;

import com.weareadaptive.cluster.services.oms.util.Side;

public record OrderCommand(double price, long size, Side side)
{
}
