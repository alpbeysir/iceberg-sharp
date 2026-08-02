using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[method: JsonConstructor]
public class PartitionSpec(List<PartitionField> fields, int? specId)
{
    public static PartitionSpec Default => new([], 0);

    [JsonPropertyName("spec-id")] public int? SpecId { get; } = specId;

    [JsonPropertyName("fields")] public List<PartitionField> Fields { get; } = fields;
}
