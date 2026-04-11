using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

/// <summary>
///     The table's last assigned partition id must match the requirement's `last-assigned-partition-id`
/// </summary>
[method: JsonConstructor]
public class AssertLastAssignedPartitionId(int lastAssignedPartitionId) : ITableRequirement
{
    [JsonPropertyName("last-assigned-partition-id")]
    public int LastAssignedPartitionId { get; } = lastAssignedPartitionId;
}