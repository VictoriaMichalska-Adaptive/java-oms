package com.weareadaptive.cluster.services.oms.util;

import java.util.HashMap;
import java.util.Map;

public enum Method
{
    PLACE((byte) 0), CANCEL((byte) 1), CLEAR((byte) 2), RESET((byte) 3), ASKS((byte) 4), BIDS((byte) 5), CURRENT_ORDER_ID((byte) 6);
    final private int value;
    private static final Map<Byte, Method> BYTE_TO_ENUM = new HashMap<>();

    static {
        for (Method serviceName : Method.values()) {
            BYTE_TO_ENUM.put(serviceName.getByte(), serviceName);
        }
    }
    Method(byte value) {
        this.value = value;
    }
    public byte getByte() {
        return (byte) value;
    }
    public static Method fromByteValue(byte byteValue) {
        Method method = BYTE_TO_ENUM.get(byteValue);
        if (method == null) {
            throw new IllegalArgumentException("Invalid byte value for Side: " + byteValue);
        }
        return method;
    }
}
