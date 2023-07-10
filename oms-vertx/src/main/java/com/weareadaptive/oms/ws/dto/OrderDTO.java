package com.weareadaptive.oms.ws.dto;
import com.weareadaptive.oms.util.Side;

public record OrderDTO(double price, long size, Side side)
{
}
