using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[method: JsonConstructor]
public class SnapshotLogEntry(long snapshotId, long timestampMs)
{
    [JsonPropertyName("snapshot-id")] public long SnapshotId { get; } = snapshotId;

    [JsonPropertyName("timestamp-ms")] public long TimestampMs { get; } = timestampMs;
}