using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

/// <summary>
///     The table's default sort order id must match the requirement's `default-sort-order-id`
/// </summary>
[method: JsonConstructor]
public class AssertDefaultSortOrderId(int defaultSortOrderId) : ITableRequirement
{
    [JsonPropertyName("default-sort-order-id")]
    public int DefaultSortOrderId { get; } = defaultSortOrderId;
}