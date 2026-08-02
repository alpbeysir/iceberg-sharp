using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Failed server-side planning result
/// </summary>
[method: JsonConstructor]
public class FailedPlanningResult(ErrorModel error, PlanStatus status) : IcebergErrorResponse(error)
{
    [JsonPropertyName("status")]
    [JsonConverter(typeof(JsonStringEnumConverter<PlanStatus>))]
    public PlanStatus Status { get; } = status;
}