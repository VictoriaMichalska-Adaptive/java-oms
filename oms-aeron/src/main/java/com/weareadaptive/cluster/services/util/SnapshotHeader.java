package com.weareadaptive.cluster.services.util;

import java.util.HashMap;
import java.util.Map;

public enum SnapshotHeader
{
    BIDS((byte) 0), ASKS((byte) 1), ORDER_IDS((byte) 2), END_OF_SNAPSHOT((byte) 3), NONE((byte) -1);
    private final int value;
    private static final Map<Byte, SnapshotHeader> BYTE_TO_ENUM = new HashMap<>();

    static {
        for (SnapshotHeader snapshotHeader : SnapshotHeader.values()) {
            BYTE_TO_ENUM.put(snapshotHeader.getByte(), snapshotHeader);
        }
    }

    SnapshotHeader(int byteValue) {
        this.value = byteValue;
    }
    public byte getByte() {
        return (byte) value;
    }
    public static SnapshotHeader fromByteValue(byte byteValue) {
        SnapshotHeader snapshotHeader = BYTE_TO_ENUM.getOrDefault(byteValue, NONE);
        if (snapshotHeader == null) {
            throw new IllegalArgumentException("Invalid byte value for SnapshotHeader: " + byteValue);
        }
        return snapshotHeader;
    }
}
