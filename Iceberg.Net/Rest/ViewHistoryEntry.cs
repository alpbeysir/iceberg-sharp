using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class ViewHistoryEntry(long timestampMs, int versionId)
{
    [JsonPropertyName("version-id")] public int VersionId { get; } = versionId;

    [JsonPropertyName("timestamp-ms")] public long TimestampMs { get; } = timestampMs;
}