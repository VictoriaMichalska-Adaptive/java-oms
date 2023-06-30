package com.weareadaptive.oms.ws;

import com.weareadaptive.oms.util.Side;

public class OrderDTO
{
    final double price;
    final long size;
    final Side side;
    public OrderDTO(double price, long size, Side side) {
        this.price = price;
        this.size = size;
        this.side = side;
    }
}
