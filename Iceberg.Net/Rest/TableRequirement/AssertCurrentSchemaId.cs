using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

/// <summary>
///     The table's current schema id must match the requirement's `current-schema-id`
/// </summary>
[method: JsonConstructor]
public class AssertCurrentSchemaId(int currentSchemaId) : ITableRequirement
{
    [JsonPropertyName("current-schema-id")]
    public int CurrentSchemaId { get; } = currentSchemaId;
}