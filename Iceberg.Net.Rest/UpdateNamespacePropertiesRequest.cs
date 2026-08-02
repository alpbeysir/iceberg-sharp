using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class UpdateNamespacePropertiesRequest(ICollection<string> removals, IDictionary<string, string> updates)
{
    [JsonPropertyName("removals")] public ICollection<string> Removals { get; } = removals;

    [JsonPropertyName("updates")] public IDictionary<string, string> Updates { get; } = updates;
}