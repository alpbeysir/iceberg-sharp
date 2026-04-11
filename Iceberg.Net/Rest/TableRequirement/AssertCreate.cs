using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

/// <summary>
///     The table must not already exist; used for create transactions
/// </summary>
[method: JsonConstructor]
public record AssertCreate() : ITableRequirement;