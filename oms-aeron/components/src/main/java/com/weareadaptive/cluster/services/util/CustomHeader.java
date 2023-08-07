package com.weareadaptive.cluster.services.util;

import com.weareadaptive.cluster.services.oms.util.Method;

public class CustomHeader
{
    private ServiceName serviceName;
    private Method method;
    private long messageId;
    public CustomHeader setProperties(ServiceName serviceName, Method method, long messageId) {
        this.serviceName = serviceName;
        this.method = method;
        this.messageId = messageId;
        return this;
    }
    public ServiceName getServiceName()
    {
        return serviceName;
    }
    public Method getMethod()
    {
        return method;
    }
    public long getMessageId()
    {
        return messageId;
    }
}
