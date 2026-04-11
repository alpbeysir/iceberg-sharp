using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class EqualityDeleteFile(
    string content,
    List<int> equalityIds,
    FileFormat fileFormat,
    string filePath,
    long fileSizeInBytes,
    string keyMetadata,
    List<bool> partition,
    long recordCount,
    int? sortOrderId,
    int specId,
    List<long> splitOffsets)
    : ContentFile(
        fileFormat,
        filePath,
        fileSizeInBytes,
        keyMetadata,
        partition,
        recordCount,
        sortOrderId,
        specId,
        splitOffsets)
{
    [JsonPropertyName("content")] public string Content { get; } = content;

    /// <summary>
    ///     List of equality field IDs
    /// </summary>
    [JsonPropertyName("equality-ids")]
    public List<int> EqualityIds { get; } = equalityIds;
}