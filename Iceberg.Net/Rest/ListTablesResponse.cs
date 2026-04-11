using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public record ListTablesResponse(
    [property: JsonPropertyName("identifiers")]
    ICollection<TableIdentifier> Identifiers,
    [property: JsonPropertyName("next-page-token")]
    string? NextPageToken);