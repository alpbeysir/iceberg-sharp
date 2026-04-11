using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class BlobMetadata(
    ICollection<int> fields,
    IDictionary<string, string> properties,
    long sequenceNumber,
    long snapshotId,
    string type)
{
    [JsonPropertyName("type")] public string Type { get; } = type;

    [JsonPropertyName("snapshot-id")] public long SnapshotId { get; } = snapshotId;

    [JsonPropertyName("sequence-number")] public long SequenceNumber { get; } = sequenceNumber;

    [JsonPropertyName("fields")] public ICollection<int> Fields { get; } = fields;

    [JsonPropertyName("properties")] public IDictionary<string, string> Properties { get; } = properties;
}