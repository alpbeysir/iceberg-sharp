using System.Text.Json.Serialization;
using Iceberg.Net.Rest;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Metadata;

public record TableMetadata
{
    private Dictionary<long, Snapshot>? _snapshotsById;
    private Dictionary<int, Schema>? _schemasById;

    [JsonInclude]
    [JsonPropertyName("format-version")]
    public required int FormatVersion { get; init; }

    [JsonInclude]
    [JsonPropertyName("table-uuid")]
    public required string TableUuid { get; init; }

    [JsonInclude]
    [JsonPropertyName("location")]
    public required string Location { get; init; }

    [JsonInclude]
    [JsonPropertyName("last-updated-ms")]
    public long? LastUpdatedMs { get; init; }

    [JsonInclude]
    [JsonPropertyName("next-row-id")]
    public long? NextRowId { get; init; }

    [JsonInclude]
    [JsonPropertyName("current-schema-id")]
    public int? CurrentSchemaId { get; init; }

    [JsonInclude]
    [JsonPropertyName("last-column-id")]
    public int? LastColumnId { get; init; }

    [JsonInclude]
    [JsonPropertyName("default-spec-id")]
    public int? DefaultSpecId { get; init; }

    [JsonInclude]
    [JsonPropertyName("last-partition-id")]
    public int? LastPartitionId { get; init; }

    [JsonInclude]
    [JsonPropertyName("default-sort-order-id")]
    public int? DefaultSortOrderId { get; init; }

    [JsonInclude]
    [JsonPropertyName("current-snapshot-id")]
    public long? CurrentSnapshotId { get; init; }

    [JsonInclude]
    [JsonPropertyName("last-sequence-number")]
    public long? LastSequenceNumber { get; init; }

    [JsonInclude]
    [JsonPropertyName("properties")]
    public IReadOnlyDictionary<string, string> Properties { get; init; } = new Dictionary<string, string>();

    [JsonInclude]
    [JsonPropertyName("schemas")]
    public IReadOnlyList<Schema> Schemas { get; init; } = [];

    [JsonInclude]
    [JsonPropertyName("partition-specs")]
    public IReadOnlyList<PartitionSpec> PartitionSpecs { get; init; } = [];

    [JsonInclude]
    [JsonPropertyName("sort-orders")]
    public IReadOnlyList<SortOrder> SortOrders { get; init; } = [];

    [JsonInclude]
    [JsonPropertyName("encryption-keys")]
    public IReadOnlyList<EncryptedKey> EncryptionKeys { get; init; } = [];

    [JsonInclude]
    [JsonPropertyName("snapshots")]
    public IReadOnlyList<Snapshot> Snapshots { get; init; } = [];

    [JsonInclude]
    [JsonPropertyName("refs")]
    public IReadOnlyDictionary<string, SnapshotReference> Refs { get; init; } =
        new Dictionary<string, SnapshotReference>();

    [JsonInclude]
    [JsonPropertyName("snapshot-log")]
    public IReadOnlyList<SnapshotLogEntry> SnapshotLog { get; init; } = [];

    [JsonInclude]
    [JsonPropertyName("metadata-log")]
    public IReadOnlyList<MetadataLogEntry> MetadataLog { get; init; } = [];

    [JsonInclude]
    [JsonPropertyName("statistics")]
    public IReadOnlyList<StatisticsFile> Statistics { get; init; } = [];

    [JsonInclude]
    [JsonPropertyName("partition-statistics")]
    public IReadOnlyList<PartitionStatisticsFile> PartitionStatistics { get; init; } = [];

    [JsonIgnore]
    public IReadOnlyDictionary<long, Snapshot> SnapshotsById =>
        _snapshotsById ??= Snapshots.ToDictionary(snapshot => snapshot.SnapshotId);

    [JsonIgnore]
    public IReadOnlyDictionary<int, Schema> SchemasById =>
        _schemasById ??= Schemas.DistinctBy(schema => schema.SchemaId!.Value)
            .ToDictionary(schema => schema.SchemaId!.Value);
}