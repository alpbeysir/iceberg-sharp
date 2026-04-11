using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

/// <summary>
///     The table UUID must match the requirement's `uuid`
/// </summary>
[method: JsonConstructor]
public class AssertTableUuid(string uuid) : ITableRequirement
{
    [JsonPropertyName("uuid")] public string Uuid { get; } = uuid;
}