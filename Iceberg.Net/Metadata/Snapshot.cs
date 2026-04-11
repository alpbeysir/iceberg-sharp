using System.Text.Json.Serialization;
using Iceberg.Net.Rest;

namespace Iceberg.Net.Metadata;

public class Snapshot
{
    [JsonPropertyName("snapshot-id")] public required long SnapshotId { get; init; }

    [JsonPropertyName("parent-snapshot-id")]
    public long? ParentSnapshotId { get; set; }

    [JsonPropertyName("sequence-number")] public long? SequenceNumber { get; set; }

    [JsonPropertyName("timestamp-ms")] public required long TimestampMs { get; init; }

    /// <summary>
    ///     Location of the snapshot's manifest list file
    /// </summary>
    [JsonPropertyName("manifest-list")]
    public required string ManifestList { get; init; }

    /// <summary>
    ///     The first _row_id assigned to the first row in the first data file in the first manifest
    /// </summary>
    [JsonPropertyName("first-row-id")]
    public long? FirstRowId { get; set; }

    /// <summary>
    ///     The upper bound of the number of rows with assigned row IDs
    /// </summary>
    [JsonPropertyName("added-rows")]
    public long? AddedRows { get; set; }

    [JsonPropertyName("summary")] public required Summary Summary { get; init; }

    [JsonPropertyName("schema-id")] public int? SchemaId { get; set; }
}