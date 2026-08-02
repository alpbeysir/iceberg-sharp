using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     JSON error payload returned in a response with further details on the error
/// </summary>
[method: JsonConstructor]
public class ErrorModel(int code, string message, List<string> stack, string type)
{
    /// <summary>
    ///     Human-readable error message
    /// </summary>
    [JsonPropertyName("message")]
    public string Message { get; } = message;

    /// <summary>
    ///     Internal type definition of the error
    /// </summary>
    [JsonPropertyName("type")]
    public string Type { get; } = type;

    /// <summary>
    ///     HTTP response code
    /// </summary>
    [JsonPropertyName("code")]
    public int Code { get; } = code;

    [JsonPropertyName("stack")] public List<string> Stack { get; } = stack;
}