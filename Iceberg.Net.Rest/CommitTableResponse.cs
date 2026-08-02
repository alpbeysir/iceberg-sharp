using System.Text.Json.Serialization;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CommitTableResponse(TableMetadata metadata, string metadataLocation, IDictionary<string, string>? config)
{
    [JsonPropertyName("metadata-location")]
    public string MetadataLocation { get; } = metadataLocation;

    [JsonPropertyName("config")] public IDictionary<string, string>? Config { get; } = config;

    [JsonPropertyName("metadata")] public TableMetadata Metadata { get; } = metadata;
}