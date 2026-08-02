using System.Runtime.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Token type identifier, from RFC 8693 Section 3
///     <br />
///     <br />See https://datatracker.ietf.org/doc/html/rfc8693#section-3
/// </summary>
public enum TokenType
{
    [EnumMember(Value = @"urn:ietf:params:oauth:token-type:access_token")]
    UrnIetfParamsOauthTokenTypeAccessToken = 0,

    [EnumMember(Value = @"urn:ietf:params:oauth:token-type:refresh_token")]
    UrnIetfParamsOauthTokenTypeRefreshToken = 1,

    [EnumMember(Value = @"urn:ietf:params:oauth:token-type:id_token")]
    UrnIetfParamsOauthTokenTypeIdToken = 2,

    [EnumMember(Value = @"urn:ietf:params:oauth:token-type:saml1")]
    UrnIetfParamsOauthTokenTypeSaml1 = 3,

    [EnumMember(Value = @"urn:ietf:params:oauth:token-type:saml2")]
    UrnIetfParamsOauthTokenTypeSaml2 = 4,

    [EnumMember(Value = @"urn:ietf:params:oauth:token-type:jwt")]
    UrnIetfParamsOauthTokenTypeJwt = 5
}