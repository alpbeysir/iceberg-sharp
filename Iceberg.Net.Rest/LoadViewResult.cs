using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Result used when a view is successfully loaded.
///     <br />
///     <br />
///     <br />The view metadata JSON is returned in the `metadata` field. The corresponding file location of view metadata
///     is returned in the `metadata-location` field.
///     <br />Clients can check whether metadata has changed by comparing metadata locations after the view has been
///     created.
///     <br />
///     <br />The `config` map returns view-specific configuration for the view's resources.
///     <br />
///     <br />The following configurations should be respected by clients:
///     <br />
///     <br />## General Configurations
///     <br />
///     <br />- `token`: Authorization bearer token to use for view requests if OAuth2 security is enabled
///     <br />
/// </summary>
[method: JsonConstructor]
public class LoadViewResult(IDictionary<string, string> config, ViewMetadata metadata, string metadataLocation)
{
    [JsonPropertyName("metadata-location")]
    public string MetadataLocation { get; } = metadataLocation;

    [JsonPropertyName("metadata")] public ViewMetadata Metadata { get; } = metadata;

    [JsonPropertyName("config")] public IDictionary<string, string> Config { get; } = config;
}