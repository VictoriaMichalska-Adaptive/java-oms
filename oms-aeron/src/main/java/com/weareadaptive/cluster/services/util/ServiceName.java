package com.weareadaptive.cluster.services.util;

import java.util.HashMap;
import java.util.Map;

public enum ServiceName
{
    OMS((byte) 2), NONE((byte) -1);
    private final int value;
    private static final Map<Byte, ServiceName> BYTE_TO_ENUM = new HashMap<>();

    static {
        for (ServiceName serviceName : ServiceName.values()) {
            BYTE_TO_ENUM.put(serviceName.getByte(), serviceName);
        }
    }

    ServiceName(int byteValue) {
        this.value = byteValue;
    }
    public byte getByte() {
        return (byte) value;
    }
    public static ServiceName fromByteValue(byte byteValue) {
        ServiceName serviceName = BYTE_TO_ENUM.getOrDefault(byteValue, NONE);
        if (serviceName == null) {
            throw new IllegalArgumentException("Invalid byte value for Side: " + byteValue);
        }
        return serviceName;
    }
}
