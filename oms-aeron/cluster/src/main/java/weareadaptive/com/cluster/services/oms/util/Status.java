package weareadaptive.com.cluster.services.oms.util;

import java.util.HashMap;
import java.util.Map;

public enum Status {
    RESTING((byte) 0),PARTIAL((byte) 1),FILLED((byte) 2),CANCELLED((byte) 3),NONE((byte) 4),SUCCESS((byte)5);
    private final int value;
    private static final Map<Byte, Status> BYTE_TO_ENUM = new HashMap<>();

    static {
        for (Status serviceName : Status.values()) {
            BYTE_TO_ENUM.put(serviceName.getByte(), serviceName);
        }
    }
    Status(int value) {
        this.value = value;
    }
    public byte getByte() {
        return (byte) value;
    }
    public static Status fromByteValue(byte byteValue) {
        Status status = BYTE_TO_ENUM.get(byteValue);
        if (status == null) {
            throw new IllegalArgumentException("Invalid byte value for Side: " + byteValue);
        }
        return status;
    }
}
