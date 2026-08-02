using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class TimerResult(long count, string timeUnit, long totalDuration)
{
    [JsonPropertyName("time-unit")] public string TimeUnit { get; } = timeUnit;

    [JsonPropertyName("count")] public long Count { get; } = count;

    [JsonPropertyName("total-duration")] public long TotalDuration { get; } = totalDuration;
}