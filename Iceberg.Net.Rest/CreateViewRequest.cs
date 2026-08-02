using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CreateViewRequest(
    string location,
    string name,
    IDictionary<string, string> properties,
    Schemas.Schema schema,
    ViewVersion viewVersion)
{
    [JsonPropertyName("name")] public string Name { get; } = name;

    [JsonPropertyName("location")] public string Location { get; } = location;

    [JsonPropertyName("schema")] public Schemas.Schema Schema { get; } = schema;

    /// <summary>
    ///     The view version to create, will replace the schema-id sent within the view-version with the id assigned to the
    ///     provided schema
    /// </summary>
    [JsonPropertyName("view-version")]
    public ViewVersion ViewVersion { get; } = viewVersion;

    [JsonPropertyName("properties")] public IDictionary<string, string> Properties { get; } = properties;
}