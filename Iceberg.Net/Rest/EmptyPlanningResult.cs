using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Empty server-side planning result
/// </summary>
[method: JsonConstructor]
public class EmptyPlanningResult(PlanStatus status)
{
    [JsonPropertyName("status")]
    [JsonConverter(typeof(JsonStringEnumConverter<PlanStatus>))]
    public PlanStatus Status { get; } = status;
}