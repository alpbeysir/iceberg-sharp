using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CountMap(ICollection<int> keys, ICollection<long> values)
{
    /// <summary>
    ///     List of integer column ids for each corresponding value
    /// </summary>
    [JsonPropertyName("keys")]
    public ICollection<int> Keys { get; } = keys;

    /// <summary>
    ///     List of Long values, matched to 'keys' by index
    /// </summary>
    [JsonPropertyName("values")]
    public ICollection<long> Values { get; } = values;
}