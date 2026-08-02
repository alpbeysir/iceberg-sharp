using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class ViewMetadata(
    int currentVersionId,
    int formatVersion,
    string location,
    IDictionary<string, string> properties,
    ICollection<Schemas.Schema> schemas,
    ICollection<ViewHistoryEntry> versionLog,
    ICollection<ViewVersion> versions,
    string viewUuid)
{
    [JsonPropertyName("view-uuid")] public string ViewUuid { get; } = viewUuid;

    [JsonPropertyName("format-version")] public int FormatVersion { get; } = formatVersion;

    [JsonPropertyName("location")] public string Location { get; } = location;

    [JsonPropertyName("current-version-id")]
    public int CurrentVersionId { get; } = currentVersionId;

    [JsonPropertyName("versions")] public ICollection<ViewVersion> Versions { get; } = versions;

    [JsonPropertyName("version-log")] public ICollection<ViewHistoryEntry> VersionLog { get; } = versionLog;

    [JsonPropertyName("schemas")] public ICollection<Schemas.Schema> Schemas { get; } = schemas;

    [JsonPropertyName("properties")] public IDictionary<string, string> Properties { get; } = properties;
}