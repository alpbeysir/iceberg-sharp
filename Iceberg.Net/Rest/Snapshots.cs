using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

public enum Snapshots
{
    [JsonStringEnumMemberName("all")] All = 0,

    [JsonStringEnumMemberName("refs")] Refs = 1
}