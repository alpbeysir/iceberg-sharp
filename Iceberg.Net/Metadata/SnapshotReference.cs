using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[method: JsonConstructor]
public class SnapshotReference(
    long? maxRefAgeMs,
    long? maxSnapshotAgeMs,
    int? minSnapshotsToKeep,
    long snapshotId,
    SnapshotReferenceType type)
{
    [JsonPropertyName("type")]
    [JsonConverter(typeof(JsonStringEnumConverter<SnapshotReferenceType>))]
    public SnapshotReferenceType Type { get; } = type;

    [JsonPropertyName("snapshot-id")] public long SnapshotId { get; } = snapshotId;

    [JsonPropertyName("max-ref-age-ms")] public long? MaxRefAgeMs { get; } = maxRefAgeMs;

    [JsonPropertyName("max-snapshot-age-ms")]
    public long? MaxSnapshotAgeMs { get; } = maxSnapshotAgeMs;

    [JsonPropertyName("min-snapshots-to-keep")]
    public int? MinSnapshotsToKeep { get; } = minSnapshotsToKeep;
}