using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class ValueMap(ICollection<int> keys, ICollection<bool> values)
{
    /// <summary>
    ///     List of integer column ids for each corresponding value
    /// </summary>
    [JsonPropertyName("keys")]
    public ICollection<int> Keys { get; } = keys;

    /// <summary>
    ///     List of primitive type values, matched to 'keys' by index
    /// </summary>
    [JsonPropertyName("values")]
    public ICollection<bool> Values { get; } = values;
}