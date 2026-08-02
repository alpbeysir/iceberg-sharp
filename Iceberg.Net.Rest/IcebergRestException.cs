using System.Text.Json;

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

public class IcebergRestException : Exception
{
    public IcebergRestException(
        string message,
        int statusCode,
        string? response,
        IReadOnlyDictionary<string, IEnumerable<string>> headers,
        Exception? innerException)
        : this(message, statusCode, response, headers, innerException, GetErrorMessage(response))
    {
    }

    private IcebergRestException(
        string message,
        int statusCode,
        string? response,
        IReadOnlyDictionary<string, IEnumerable<string>> headers,
        Exception? innerException,
        string? serverMessage)
        : base(FormatMessage(message, serverMessage, statusCode, response), innerException)
    {
        StatusCode = statusCode;
        Response = response;
        Headers = headers;
        ServerMessage = serverMessage;
    }

    public int StatusCode { get; }

    public string? Response { get; }

    public IReadOnlyDictionary<string, IEnumerable<string>> Headers { get; }

    /// <summary>
    ///     The human-readable message supplied by the server, when the response contains one.
    /// </summary>
    public string? ServerMessage { get; }

    public override string ToString()
    {
        return $"HTTP Response: \n\n{Response}\n\n{base.ToString()}";
    }

    private static string FormatMessage(
        string message,
        string? serverMessage,
        int statusCode,
        string? response)
    {
        string errorMessage = string.IsNullOrWhiteSpace(serverMessage) ? message : serverMessage;

        return errorMessage + "\n\nStatus: " + statusCode + "\nResponse: \n" + (response == null
            ? "(null)"
            : response[..Math.Min(response.Length, 512)]);
    }

    private static string? GetErrorMessage(string? response)
    {
        if (string.IsNullOrWhiteSpace(response)) return null;

        try
        {
            using JsonDocument document = JsonDocument.Parse(response);
            JsonElement root = document.RootElement;
            if (TryGetMessage(root, out string? errorMessage)) return errorMessage;

            if (root.ValueKind == JsonValueKind.Object &&
                root.TryGetProperty("error", out JsonElement error) &&
                TryGetMessage(error, out errorMessage))
                return errorMessage;
        }
        catch (JsonException)
        {
            // Non-JSON error responses retain the client-generated status message.
        }

        return null;
    }

    private static bool TryGetMessage(JsonElement element, out string? message)
    {
        message = null;
        if (element.ValueKind != JsonValueKind.Object ||
            !element.TryGetProperty("message", out JsonElement messageElement) ||
            messageElement.ValueKind != JsonValueKind.String)
            return false;

        message = messageElement.GetString();
        return !string.IsNullOrWhiteSpace(message);
    }
}
