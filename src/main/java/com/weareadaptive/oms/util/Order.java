package com.weareadaptive.oms.util;

public class Order
{
    final long orderId;
    final double price;
    final long size;
    public Order(long orderId, double price, long size)
    {
        this.orderId = orderId;
        this.price = price;
        this.size = size;
    }
    public double getPrice() { return price; }
    public long getSize() { return size; }
}
