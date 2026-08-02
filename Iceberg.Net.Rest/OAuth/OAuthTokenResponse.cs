using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.OAuth;

/// <summary>
///     The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of
///     the endpoint.
/// </summary>
[Obsolete]
[method: JsonConstructor]
public class OAuthTokenResponse(
    string accessToken,
    int? expiresIn,
    TokenType? issuedTokenType,
    string refreshToken,
    string scope,
    OAuthTokenResponseTokenType tokenType)
{
    /// <summary>
    ///     The access token, for client credentials or token exchange
    /// </summary>
    [JsonPropertyName("access_token")]
    public string AccessToken { get; } = accessToken;

    /// <summary>
    ///     Access token type for client credentials or token exchange
    ///     <br />
    ///     <br />See https://datatracker.ietf.org/doc/html/rfc6749#section-7.1
    /// </summary>
    [JsonPropertyName("token_type")]
    [JsonConverter(typeof(JsonStringEnumConverter<OAuthTokenResponseTokenType>))]
    public OAuthTokenResponseTokenType TokenType { get; } = tokenType;

    /// <summary>
    ///     Lifetime of the access token in seconds for client credentials or token exchange
    /// </summary>
    [JsonPropertyName("expires_in")]
    public int? ExpiresIn { get; } = expiresIn;

    [JsonPropertyName("issued_token_type")]
    [JsonConverter(typeof(JsonStringEnumConverter<TokenType>))]
    public TokenType? IssuedTokenType { get; } = issuedTokenType;

    /// <summary>
    ///     Refresh token for client credentials or token exchange
    /// </summary>
    [JsonPropertyName("refresh_token")]
    public string RefreshToken { get; } = refreshToken;

    /// <summary>
    ///     Authorization scope for client credentials or token exchange
    /// </summary>
    [JsonPropertyName("scope")]
    public string Scope { get; } = scope;
}