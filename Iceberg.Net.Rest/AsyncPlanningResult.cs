using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class AsyncPlanningResult(string planId, PlanStatus status)
{
    [JsonPropertyName("status")]
    [JsonConverter(typeof(JsonStringEnumConverter<PlanStatus>))]
    public PlanStatus Status { get; } = status;

    /// <summary>
    ///     ID used to track a planning request
    /// </summary>
    [JsonPropertyName("plan-id")]
    public string PlanId { get; } = planId;
}