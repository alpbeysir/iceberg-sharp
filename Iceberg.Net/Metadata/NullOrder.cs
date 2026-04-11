using System.Runtime.Serialization;
using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

public enum NullOrder
{
    [JsonStringEnumMemberName("nulls-first")] NullsFirst = 0,
    [JsonStringEnumMemberName("nulls-last")] NullsLast = 1
}