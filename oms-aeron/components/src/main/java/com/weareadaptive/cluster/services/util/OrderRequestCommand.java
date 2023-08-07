package com.weareadaptive.cluster.services.util;

import com.weareadaptive.cluster.services.oms.util.Side;

public class OrderRequestCommand
{
    double price;
    long size;
    Side side;

    public OrderRequestCommand setProperties(double price, long size, Side side) {
        this.price = price;
        this.size = size;
        this.side = side;
        return this;
    }

    public double getPrice()
    {
        return price;
    }

    public long getSize()
    {
        return size;
    }

    public Side getSide()
    {
        return side;
    }
}
