using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

/// <summary>
///     The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`.
///     <br />The `snapshot-id` field is required in this object, but in the case of a `null`
///     <br />the ref must not already exist.
///     <br />
/// </summary>
[method: JsonConstructor]
public class AssertRefSnapshotId(string @ref, long? snapshotId) : ITableRequirement
{
    [JsonPropertyName("ref")] public string Ref { get; } = @ref;

    [JsonPropertyName("snapshot-id")] public long? SnapshotId { get; } = snapshotId;
}