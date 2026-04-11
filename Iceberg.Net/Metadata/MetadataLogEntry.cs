using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[method: JsonConstructor]
public class MetadataLogEntry(string metadataFile, long timestampMs)
{
    [JsonPropertyName("metadata-file")] public string MetadataFile { get; } = metadataFile;

    [JsonPropertyName("timestamp-ms")] public long TimestampMs { get; } = timestampMs;
}