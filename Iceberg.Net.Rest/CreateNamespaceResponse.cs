using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public record CreateNamespaceResponse(
    [property: JsonPropertyName("namespace")]
    Namespace Ns,
    [property: JsonPropertyName("properties")]
    IDictionary<string, string> Properties);