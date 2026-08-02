using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.OAuth;

/// <summary>
///     The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of
///     the endpoint.
///     <br />
///     <br />OAuth2 token exchange request
///     <br />
///     <br />See https://datatracker.ietf.org/doc/html/rfc8693
/// </summary>
[Obsolete]
[method: JsonConstructor]
public class OAuthTokenExchangeRequest(
    string actorToken,
    TokenType? actorTokenType,
    OAuthTokenExchangeRequestGrantType grantType,
    TokenType? requestedTokenType,
    string scope,
    string subjectToken,
    TokenType subjectTokenType)
{
    [JsonPropertyName("grant_type")]
    [JsonConverter(typeof(JsonStringEnumConverter<OAuthTokenExchangeRequestGrantType>))]
    public OAuthTokenExchangeRequestGrantType GrantType { get; } = grantType;

    [JsonPropertyName("scope")] public string Scope { get; } = scope;

    [JsonPropertyName("requested_token_type")]
    [JsonConverter(typeof(JsonStringEnumConverter<TokenType>))]
    public TokenType? RequestedTokenType { get; } = requestedTokenType;

    /// <summary>
    ///     Subject token for token exchange request
    /// </summary>
    [JsonPropertyName("subject_token")]
    public string SubjectToken { get; } = subjectToken;

    [JsonPropertyName("subject_token_type")]
    [JsonConverter(typeof(JsonStringEnumConverter<TokenType>))]
    public TokenType SubjectTokenType { get; } = subjectTokenType;

    /// <summary>
    ///     Actor token for token exchange request
    /// </summary>
    [JsonPropertyName("actor_token")]
    public string ActorToken { get; } = actorToken;

    [JsonPropertyName("actor_token_type")]
    [JsonConverter(typeof(JsonStringEnumConverter<TokenType>))]
    public TokenType? ActorTokenType { get; } = actorTokenType;
}