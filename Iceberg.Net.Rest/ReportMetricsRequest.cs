using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class ReportMetricsRequest(string reportType)
{
    [JsonPropertyName("report-type")] public string ReportType { get; } = reportType;
}