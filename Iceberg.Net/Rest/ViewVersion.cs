using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class ViewVersion(
    string defaultCatalog,
    Namespace defaultNamespace,
    ICollection<SqlViewRepresentation> representations,
    int schemaId,
    IDictionary<string, string> summary,
    long timestampMs,
    int versionId)
{
    [JsonPropertyName("version-id")] public int VersionId { get; } = versionId;

    [JsonPropertyName("timestamp-ms")] public long TimestampMs { get; } = timestampMs;

    /// <summary>
    ///     Schema ID to set as current, or -1 to set last added schema
    /// </summary>
    [JsonPropertyName("schema-id")]
    public int SchemaId { get; } = schemaId;

    [JsonPropertyName("summary")] public IDictionary<string, string> Summary { get; } = summary;

    [JsonPropertyName("representations")]
    public ICollection<SqlViewRepresentation> Representations { get; } = representations;

    [JsonPropertyName("default-catalog")] public string DefaultCatalog { get; } = defaultCatalog;

    [JsonPropertyName("default-namespace")]
    public Namespace DefaultNamespace { get; } = defaultNamespace;
}