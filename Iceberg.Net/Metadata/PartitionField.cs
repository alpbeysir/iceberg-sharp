using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[method: JsonConstructor]
public class PartitionField(int? fieldId, string name, int sourceId, string transform)
{
    [JsonPropertyName("field-id")] public int? FieldId { get; } = fieldId;

    [JsonPropertyName("source-id")] public int SourceId { get; } = sourceId;

    [JsonPropertyName("name")] public string Name { get; } = name;

    [JsonPropertyName("transform")] public string Transform { get; } = transform;
}