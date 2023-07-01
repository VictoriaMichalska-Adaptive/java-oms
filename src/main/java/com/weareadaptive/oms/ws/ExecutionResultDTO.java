package com.weareadaptive.oms.ws;

import com.weareadaptive.oms.util.Status;

public record ExecutionResultDTO(Status status, Long orderId)
{
}
