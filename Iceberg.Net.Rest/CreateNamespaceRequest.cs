using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CreateNamespaceRequest(Namespace ns, Dictionary<string, string> properties)
{
    [JsonPropertyName("namespace")] public Namespace Namespace { get; } = ns;

    /// <summary>
    ///     Configured string to string map of properties for the namespace
    /// </summary>
    [JsonPropertyName("properties")]
    public IDictionary<string, string> Properties { get; } = properties;
}