package weareadaptive.com.cluster.services.oms.util;

import java.util.HashMap;
import java.util.Map;

public enum Side {
    BID((byte) 0),ASK((byte) 1);
    private final int value;
    private static final Map<Byte, Side> BYTE_TO_ENUM = new HashMap<>();

    static {
        for (Side serviceName : Side.values()) {
            BYTE_TO_ENUM.put(serviceName.getByte(), serviceName);
        }
    }
    Side(byte value) {
        this.value = value;
    }
    public byte getByte() {
        return (byte) value;
    }
    public static Side fromByteValue(byte byteValue)
    {
        Side side = BYTE_TO_ENUM.get(byteValue);
        if (side == null)
        {
            throw new IllegalArgumentException("Invalid byte value for Side: " + byteValue);
        }
        return side;
    }
}
