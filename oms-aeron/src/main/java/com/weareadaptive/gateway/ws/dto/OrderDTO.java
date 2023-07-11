package com.weareadaptive.gateway.ws.dto;

import com.weareadaptive.cluster.services.oms.util.Side;

public record OrderDTO(double price, long size, Side side)
{
}
