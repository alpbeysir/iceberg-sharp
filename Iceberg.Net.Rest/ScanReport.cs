using System.Text.Json.Serialization;
using Iceberg.Net.Rest.Expression;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class ScanReport(
    IExpression filter,
    IDictionary<string, string> metadata,
    Metrics metrics,
    List<int> projectedFieldIds,
    List<string> projectedFieldNames,
    int schemaId,
    long snapshotId,
    string tableName)
{
    [JsonPropertyName("table-name")] public string TableName { get; } = tableName;

    [JsonPropertyName("snapshot-id")] public long SnapshotId { get; } = snapshotId;

    [JsonPropertyName("filter")] public IExpression Filter { get; } = filter;

    [JsonPropertyName("schema-id")] public int SchemaId { get; } = schemaId;

    [JsonPropertyName("projected-field-ids")]
    public List<int> ProjectedFieldIds { get; } = projectedFieldIds;

    [JsonPropertyName("projected-field-names")]
    public List<string> ProjectedFieldNames { get; } = projectedFieldNames;

    [JsonPropertyName("metrics")] public Metrics Metrics { get; } = metrics;

    [JsonPropertyName("metadata")] public IDictionary<string, string> Metadata { get; } = metadata;
}