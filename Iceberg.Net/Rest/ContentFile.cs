using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "content")]
[JsonDerivedType(typeof(DeleteFile), "position-deletes")]
[JsonDerivedType(typeof(DataFile), "data")]
[JsonDerivedType(typeof(EqualityDeleteFile), "equality-deletes")]
[method: JsonConstructor]
public class ContentFile(
    FileFormat fileFormat,
    string filePath,
    long fileSizeInBytes,
    string keyMetadata,
    ICollection<bool> partition,
    long recordCount,
    int? sortOrderId,
    int specId,
    ICollection<long> splitOffsets)
{
    [JsonPropertyName("file-path")] public string FilePath { get; } = filePath;

    [JsonPropertyName("file-format")]
    [JsonConverter(typeof(JsonStringEnumConverter<FileFormat>))]
    public FileFormat FileFormat { get; } = fileFormat;

    [JsonPropertyName("spec-id")] public int SpecId { get; } = specId;

    /// <summary>
    ///     A list of partition field values ordered based on the fields of the partition spec specified by the `spec-id`
    /// </summary>
    [JsonPropertyName("partition")]
    public ICollection<bool> Partition { get; } = partition;

    /// <summary>
    ///     Total file size in bytes
    /// </summary>
    [JsonPropertyName("file-size-in-bytes")]
    public long FileSizeInBytes { get; } = fileSizeInBytes;

    /// <summary>
    ///     Number of records in the file
    /// </summary>
    [JsonPropertyName("record-count")]
    public long RecordCount { get; } = recordCount;

    /// <summary>
    ///     Encryption key metadata blob
    /// </summary>
    [JsonPropertyName("key-metadata")]
    public string KeyMetadata { get; } = keyMetadata;

    /// <summary>
    ///     List of splittable offsets
    /// </summary>
    [JsonPropertyName("split-offsets")]
    public ICollection<long> SplitOffsets { get; } = splitOffsets;

    [JsonPropertyName("sort-order-id")] public int? SortOrderId { get; } = sortOrderId;
}