using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     The view UUID must match the requirement's `uuid`
/// </summary>
public class ViewRequirement
{
    [JsonConstructor]
    public ViewRequirement()
    {
    }
}