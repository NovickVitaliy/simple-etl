namespace simple_etl.ErrorHandling;

public class ErrorOr<T>
{
    public T Data { get; }
    public string ErrorMessage { get; }
    
    private ErrorOr(T data, string errorMessage = "")
    {
        Data = data;
        ErrorMessage = errorMessage;
    }

    public static ErrorOr<T> Success(T data)
    {
        return new ErrorOr<T>(data);
    }

    public static ErrorOr<T> Failure(string errorMessage)
    {
        return new ErrorOr<T>(default!, errorMessage);
    }
}