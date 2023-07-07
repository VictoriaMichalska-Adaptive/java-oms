package com.weareadaptive.oms.ws.exception;

public class MissingFieldException extends RuntimeException
{
    private final String missingField;

    public MissingFieldException(final String missingField)
    {
        this.missingField = missingField;
    }

    @Override
    public String getMessage()
    {
        return missingField;
    }
}
