using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public record TableIdentifier(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("namespace")]
    Namespace Ns);