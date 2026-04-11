using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class FetchScanTasksRequest(string planTask)
{
    [JsonPropertyName("plan-task")] public string PlanTask { get; } = planTask;
}