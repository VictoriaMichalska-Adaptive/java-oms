package com.weareadaptive.gateway.ws.command;


import com.weareadaptive.cluster.services.oms.util.Status;

public record ExecutionResultCommand(Status status, Long orderId)
{
}
