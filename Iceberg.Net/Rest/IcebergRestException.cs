namespace Iceberg.Net.Rest;

#pragma warning disable 1573, 1591
public class IcebergRestException<TResult>(
    string message,
    int statusCode,
    string response,
    IReadOnlyDictionary<string, IEnumerable<string>> headers,
    TResult result,
    Exception? innerException)
    : IcebergRestException(message, statusCode, response, headers, innerException)
{
    public TResult Result { get; private set; } = result;
}
#pragma warning restore 1573, 1591

public class IcebergRestException(
    string message,
    int statusCode,
    string? response,
    IReadOnlyDictionary<string, IEnumerable<string>> headers,
    Exception? innerException)
    : Exception(
        message + "\n\nStatus: " + statusCode + "\nResponse: \n" + (response == null
            ? "(null)"
            : response.Substring(0, response.Length >= 512 ? 512 : response.Length)),
        innerException)
{
    public int StatusCode { get; private set; } = statusCode;

    public string? Response { get; } = response;

    public IReadOnlyDictionary<string, IEnumerable<string>> Headers { get; private set; } = headers;

    public override string ToString()
    {
        return $"HTTP Response: \n\n{Response}\n\n{base.ToString()}";
    }
}