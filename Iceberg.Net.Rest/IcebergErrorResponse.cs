using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     JSON wrapper for all error responses (non-2xx)
/// </summary>
[method: JsonConstructor]
public class IcebergErrorResponse(ErrorModel error)
{
    [JsonPropertyName("error")] public ErrorModel Error { get; } = error;
}