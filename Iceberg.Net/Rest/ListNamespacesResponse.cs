using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public record ListNamespacesResponse(
    [property: JsonPropertyName("namespaces")]
    ICollection<Namespace> Namespaces,
    [property: JsonPropertyName("next-page-token")]
    string? NextPageToken);