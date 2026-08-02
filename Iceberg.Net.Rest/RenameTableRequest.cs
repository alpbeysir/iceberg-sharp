using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class RenameTableRequest(TableIdentifier destination, TableIdentifier source)
{
    [JsonPropertyName("source")] public TableIdentifier Source { get; } = source;

    [JsonPropertyName("destination")] public TableIdentifier Destination { get; } = destination;
}