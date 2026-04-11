using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[method: JsonConstructor]
public class PartitionSpec(List<PartitionField> fields, int? specId)
{
    [JsonPropertyName("spec-id")] public int? SpecId { get; } = specId;

    [JsonPropertyName("fields")] public List<PartitionField> Fields { get; } = fields;
}