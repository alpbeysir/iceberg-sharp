using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

/// <summary>
///     The table's default spec id must match the requirement's `default-spec-id`
/// </summary>
[method: JsonConstructor]
public class AssertDefaultSpecId(int defaultSpecId) : ITableRequirement
{
    [JsonPropertyName("default-spec-id")] public int DefaultSpecId { get; } = defaultSpecId;
}