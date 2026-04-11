using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

/// <summary>
///     The table's last assigned column id must match the requirement's `last-assigned-field-id`
/// </summary>
[method: JsonConstructor]
public class AssertLastAssignedFieldId(int lastAssignedFieldId) : ITableRequirement
{
    [JsonPropertyName("last-assigned-field-id")]
    public int LastAssignedFieldId { get; } = lastAssignedFieldId;
}