using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[method: JsonConstructor]
public class SortOrder(List<SortField> fields, int orderId)
{
    public static SortOrder Default => new([], 0);

    [JsonPropertyName("order-id")] public int OrderId { get; } = orderId;

    [JsonPropertyName("fields")] public List<SortField> Fields { get; } = fields;
}
