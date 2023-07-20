package com.weareadaptive.util;

import com.weareadaptive.cluster.services.oms.util.Side;

public record OrderRequest(double price, long size, Side side)
{
}
