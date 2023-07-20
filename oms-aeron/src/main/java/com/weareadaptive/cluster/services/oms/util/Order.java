package com.weareadaptive.cluster.services.oms.util;

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
        var comparePrices = Double.compare(order.getPrice(), price);
        if (comparePrices == 0) {
            return -1 * Double.compare(order.getOrderId(), orderId);
        }
        return comparePrices;
    }
}
