using System.Runtime.Serialization;

namespace Iceberg.Net.Rest.OAuth;

public enum OAuthTokenExchangeRequestGrantType
{
    [EnumMember(Value = @"urn:ietf:params:oauth:grant-type:token-exchange")]
    UrnIetfParamsOauthGrantTypeTokenExchange = 0
}