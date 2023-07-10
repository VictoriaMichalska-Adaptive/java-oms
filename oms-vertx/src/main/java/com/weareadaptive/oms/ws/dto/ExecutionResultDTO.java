package com.weareadaptive.oms.ws.dto;

import com.weareadaptive.oms.util.Status;

public record ExecutionResultDTO(Status status, Long orderId)
{
}
