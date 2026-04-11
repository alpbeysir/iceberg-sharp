using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public record UpdateNamespacePropertiesResponse(
    ICollection<string> Missing,
    ICollection<string> Removed,
    ICollection<string> Updated)
{
    /// <summary>
    ///     List of property keys that were added or updated
    /// </summary>
    [JsonPropertyName("updated")]
    public ICollection<string> Updated { get; } = Updated;

    /// <summary>
    ///     List of properties that were removed
    /// </summary>
    [JsonPropertyName("removed")]
    public ICollection<string> Removed { get; } = Removed;

    /// <summary>
    ///     List of properties requested for removal that were not found in the namespace's properties. Represents a partial
    ///     success response. Servers do not need to implement this.
    /// </summary>
    [JsonPropertyName("missing")]
    public ICollection<string> Missing { get; } = Missing;
}