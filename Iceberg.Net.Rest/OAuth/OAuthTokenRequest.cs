using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.OAuth;

/// <summary>
///     The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of
///     the endpoint.
/// </summary>
[Obsolete]
public class OAuthTokenRequest
{
    [JsonConstructor]
    public OAuthTokenRequest()
    {
    }
}