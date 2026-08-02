using System.Runtime.Serialization;

namespace Iceberg.Net.Rest.OAuth;

public enum OAuthErrorError
{
    [EnumMember(Value = @"invalid_request")]
    InvalidRequest = 0,

    [EnumMember(Value = @"invalid_client")]
    InvalidClient = 1,

    [EnumMember(Value = @"invalid_grant")] InvalidGrant = 2,

    [EnumMember(Value = @"unauthorized_client")]
    UnauthorizedClient = 3,

    [EnumMember(Value = @"unsupported_grant_type")]
    UnsupportedGrantType = 4,

    [EnumMember(Value = @"invalid_scope")] InvalidScope = 5
}