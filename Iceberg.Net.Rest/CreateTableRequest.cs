using System.Text.Json.Serialization;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Rest;

public record CreateTableRequest
{
    [JsonPropertyName("name")] public required string Name { get; init; }

    [JsonPropertyName("schema")] public required Schemas.Schema Schema { get; init; }

    [JsonPropertyName("location")] public string? Location { get; set; }

    [JsonPropertyName("partition-spec")] public PartitionSpec? PartitionSpec { get; set; }

    [JsonPropertyName("write-order")] public SortOrder? WriteOrder { get; set; }

    [JsonPropertyName("stage-create")] public bool? StageCreate { get; set; }

    [JsonPropertyName("properties")] public IDictionary<string, string>? Properties { get; set; }
}