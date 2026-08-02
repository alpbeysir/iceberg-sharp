using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.OAuth;

/// <summary>
///     The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of
///     the endpoint.
/// </summary>
[method: JsonConstructor]
public class OAuthError(OAuthErrorError error, string errorDescription, string errorUri)
{
    [JsonPropertyName("error")]
    [JsonConverter(typeof(JsonStringEnumConverter<OAuthErrorError>))]
    public OAuthErrorError Error { get; } = error;

    [JsonPropertyName("error_description")]
    public string ErrorDescription { get; } = errorDescription;

    [JsonPropertyName("error_uri")] public string ErrorUri { get; } = errorUri;
}