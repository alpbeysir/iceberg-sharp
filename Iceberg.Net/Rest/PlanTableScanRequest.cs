using System.Text.Json.Serialization;
using Iceberg.Net.Rest.Expression;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class PlanTableScanRequest(
    bool? caseSensitive,
    long? endSnapshotId,
    IExpression filter,
    long? minRowsRequested,
    ICollection<string> select,
    long? snapshotId,
    long? startSnapshotId,
    ICollection<string> statsFields,
    bool? useSnapshotSchema)
{
    /// <summary>
    ///     Identifier for the snapshot to scan in a point-in-time scan
    /// </summary>
    [JsonPropertyName("snapshot-id")]
    public long? SnapshotId { get; } = snapshotId;

    /// <summary>
    ///     List of selected schema fields
    /// </summary>
    [JsonPropertyName("select")]
    public ICollection<string> Select { get; } = select;

    /// <summary>
    ///     Expression used to filter the table data
    /// </summary>
    [JsonPropertyName("filter")]
    public IExpression Filter { get; } = filter;

    /// <summary>
    ///     The minimum number of rows requested for the scan. This is used as a hint to the server to not have to return more
    ///     rows than necessary. It is not required for the server to return that many rows since the scan may not produce that
    ///     many rows. The server can also return more rows than requested.
    /// </summary>
    [JsonPropertyName("min-rows-requested")]
    public long? MinRowsRequested { get; } = minRowsRequested;

    /// <summary>
    ///     Enables case sensitive field matching for filter and select
    /// </summary>
    [JsonPropertyName("case-sensitive")]
    public bool? CaseSensitive { get; } = caseSensitive;

    /// <summary>
    ///     Whether to use the schema at the time the snapshot was written.
    ///     <br />When time travelling, the snapshot schema should be used (true). When scanning a branch, the table schema
    ///     should be used (false).
    /// </summary>
    [JsonPropertyName("use-snapshot-schema")]
    public bool? UseSnapshotSchema { get; } = useSnapshotSchema;

    /// <summary>
    ///     Starting snapshot ID for an incremental scan (exclusive)
    /// </summary>
    [JsonPropertyName("start-snapshot-id")]
    public long? StartSnapshotId { get; } = startSnapshotId;

    /// <summary>
    ///     Ending snapshot ID for an incremental scan (inclusive).
    ///     <br />Required when start-snapshot-id is specified.
    /// </summary>
    [JsonPropertyName("end-snapshot-id")]
    public long? EndSnapshotId { get; } = endSnapshotId;

    /// <summary>
    ///     List of fields for which the service should send column stats.
    /// </summary>
    [JsonPropertyName("stats-fields")]
    public ICollection<string> StatsFields { get; } = statsFields;
}