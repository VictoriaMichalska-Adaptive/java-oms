package weareadaptive.com.gateway.exception;

public class BadFieldException extends RuntimeException
{
    private final String missingField;

    public BadFieldException(final String missingField)
    {
        this.missingField = missingField;
    }

    @Override
    public String getMessage()
    {
        return missingField;
    }
}
