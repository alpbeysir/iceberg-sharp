using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class RegisterTableRequest(string metadataLocation, string name, bool? overwrite)
{
    [JsonPropertyName("name")] public string Name { get; } = name;

    [JsonPropertyName("metadata-location")]
    public string MetadataLocation { get; } = metadataLocation;

    /// <summary>
    ///     Whether to overwrite table metadata if the table already exists
    /// </summary>
    [JsonPropertyName("overwrite")]
    public bool? Overwrite { get; } = overwrite;
}