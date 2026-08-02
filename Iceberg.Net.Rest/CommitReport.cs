using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CommitReport(
    IDictionary<string, string> metadata,
    Metrics metrics,
    string operation,
    long sequenceNumber,
    long snapshotId,
    string tableName)
{
    [JsonPropertyName("table-name")] public string TableName { get; } = tableName;

    [JsonPropertyName("snapshot-id")] public long SnapshotId { get; } = snapshotId;

    [JsonPropertyName("sequence-number")] public long SequenceNumber { get; } = sequenceNumber;

    [JsonPropertyName("operation")] public string Operation { get; } = operation;

    [JsonPropertyName("metrics")] public Metrics Metrics { get; } = metrics;

    [JsonPropertyName("metadata")] public IDictionary<string, string> Metadata { get; } = metadata;
}