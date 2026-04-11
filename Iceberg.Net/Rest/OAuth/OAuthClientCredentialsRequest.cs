using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.OAuth;

/// <summary>
///     The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of
///     the endpoint.
///     <br />
///     <br />OAuth2 client credentials request
///     <br />
///     <br />See https://datatracker.ietf.org/doc/html/rfc6749#section-4.4
/// </summary>
[Obsolete]
[method: JsonConstructor]
public class OAuthClientCredentialsRequest(
    string clientId,
    string clientSecret,
    OAuthClientCredentialsRequestGrantType grantType,
    string scope)
{
    [JsonPropertyName("grant_type")]
    [JsonConverter(typeof(JsonStringEnumConverter<OAuthClientCredentialsRequestGrantType>))]
    public OAuthClientCredentialsRequestGrantType GrantType { get; } = grantType;

    [JsonPropertyName("scope")] public string Scope { get; } = scope;

    /// <summary>
    ///     Client ID
    ///     <br />
    ///     <br />This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.
    /// </summary>
    [JsonPropertyName("client_id")]
    public string ClientId { get; } = clientId;

    /// <summary>
    ///     Client secret
    ///     <br />
    ///     <br />This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header.
    /// </summary>
    [JsonPropertyName("client_secret")]
    public string ClientSecret { get; } = clientSecret;
}