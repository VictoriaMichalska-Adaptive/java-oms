package com.weareadaptive.cluster.services.oms.ws.dto;


import com.weareadaptive.cluster.services.oms.util.Status;

public record ExecutionResultDTO(Status status, Long orderId)
{
}
