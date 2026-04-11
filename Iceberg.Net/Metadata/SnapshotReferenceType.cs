using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

public enum SnapshotReferenceType
{
    [JsonStringEnumMemberName("tag")] Tag = 0,

    [JsonStringEnumMemberName("branch")] Branch = 1
}