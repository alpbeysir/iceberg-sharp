using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CounterResult(string unit, long value)
{
    [JsonPropertyName("unit")] public string Unit { get; } = unit;

    [JsonPropertyName("value")] public long Value { get; } = value;
}