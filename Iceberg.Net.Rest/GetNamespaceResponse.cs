using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class GetNamespaceResponse(Namespace ns, IDictionary<string, string> properties)
{
    [JsonPropertyName("namespace")] public Namespace Namespace { get; } = ns;

    /// <summary>
    ///     Properties stored on the namespace, if supported by the server. If the server does not support namespace
    ///     properties, it should return null for this field. If namespace properties are supported, but none are set, it
    ///     should return an empty object.
    /// </summary>
    [JsonPropertyName("properties")]
    public IDictionary<string, string> Properties { get; } = properties;
}