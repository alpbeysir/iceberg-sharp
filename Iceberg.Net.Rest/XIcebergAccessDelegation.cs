using System.Runtime.Serialization;
using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

public enum XIcebergAccessDelegation
{
    [JsonStringEnumMemberName("vended-credentials")] [EnumMember]
    VendedCredentials = 0,

    [JsonStringEnumMemberName("remote-signing")] [EnumMember]
    RemoteSigning = 1
}