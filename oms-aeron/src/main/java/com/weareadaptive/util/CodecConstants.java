package com.weareadaptive.util;

public class CodecConstants
{
    public final static int METHOD_NAME_SIZE = Byte.BYTES;
    public final static int SERVICE_NAME_SIZE = Byte.BYTES;
    public final static int HEADER_SIZE = Byte.BYTES + Byte.BYTES + Long.BYTES;
    public final static int ORDER_REQUEST_SIZE = Double.BYTES + Long.BYTES + Byte.BYTES;
    public final static int ID_SIZE = Long.BYTES;
    public final static int EXECUTION_RESULT_SIZE = Long.BYTES + Long.BYTES + Byte.BYTES;
    public final static int SUCCESS_MESSAGE_SIZE = Long.BYTES + Byte.BYTES;
}
