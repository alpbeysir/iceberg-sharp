using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[method: JsonConstructor]
public class SortField(SortDirection direction, NullOrder nullOrder, int sourceId, string transform)
{
    [JsonPropertyName("source-id")] public int SourceId { get; } = sourceId;

    [JsonPropertyName("transform")] public string Transform { get; } = transform;

    [JsonPropertyName("direction")]
    [JsonConverter(typeof(JsonStringEnumConverter<SortDirection>))]
    public SortDirection Direction { get; } = direction;

    [JsonPropertyName("null-order")]
    [JsonConverter(typeof(JsonStringEnumConverter<NullOrder>))]
    public NullOrder NullOrder { get; } = nullOrder;
}