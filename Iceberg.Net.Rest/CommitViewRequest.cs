using System.Text.Json.Serialization;
using Iceberg.Net.Rest.TableRequirement;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CommitViewRequest(
    TableIdentifier identifier,
    ICollection<ITableRequirement> requirements,
    ICollection<ViewUpdate> updates)
{
    /// <summary>
    ///     View identifier to update
    /// </summary>
    [JsonPropertyName("identifier")]
    public TableIdentifier Identifier { get; } = identifier;

    [JsonPropertyName("requirements")] public ICollection<ITableRequirement> Requirements { get; } = requirements;

    [JsonPropertyName("updates")] public ICollection<ViewUpdate> Updates { get; } = updates;
}