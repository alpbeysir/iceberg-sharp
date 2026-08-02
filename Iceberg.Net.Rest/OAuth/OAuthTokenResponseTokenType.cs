using System.Runtime.Serialization;

namespace Iceberg.Net.Rest.OAuth;

public enum OAuthTokenResponseTokenType
{
    [EnumMember(Value = @"bearer")] Bearer = 0,

    [EnumMember(Value = @"mac")] Mac = 1,

    [EnumMember(Value = @"N_A")] NA = 2
}