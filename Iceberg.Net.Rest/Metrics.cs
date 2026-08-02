using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

public class Metrics : Dictionary<string, MetricResult>
{
    [JsonConstructor]
    public Metrics()
    {
    }
}