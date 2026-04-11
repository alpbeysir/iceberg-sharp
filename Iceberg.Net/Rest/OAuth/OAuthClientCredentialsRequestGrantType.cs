using System.Runtime.Serialization;

namespace Iceberg.Net.Rest.OAuth;

public enum OAuthClientCredentialsRequestGrantType
{
    [EnumMember(Value = @"client_credentials")]
    ClientCredentials = 0
}