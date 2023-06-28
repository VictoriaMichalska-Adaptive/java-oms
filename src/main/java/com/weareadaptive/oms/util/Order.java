package com.weareadaptive.oms.util;

public class Order implements Comparable<Order>
{
    final long orderId;
    final double price;
    long size;
    public Order(long orderId, double price, long size)
    {
        this.orderId = orderId;
        this.price = price;
        this.size = size;
    }
    public double getPrice() { return price; }
    public long getSize() { return size; }
    public void setSize(long newSize) { size = newSize; }
    public long getOrderId() { return orderId; }

    @Override
    public int compareTo(Order order)
    {
        return Double.compare(order.getPrice(), price);
    }
}
