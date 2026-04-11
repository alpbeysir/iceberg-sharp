using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class StatisticsFile(
    ICollection<BlobMetadata> blobMetadata,
    long fileFooterSizeInBytes,
    long fileSizeInBytes,
    long snapshotId,
    string statisticsPath)
{
    [JsonPropertyName("snapshot-id")] public long SnapshotId { get; } = snapshotId;

    [JsonPropertyName("statistics-path")] public string StatisticsPath { get; } = statisticsPath;

    [JsonPropertyName("file-size-in-bytes")]
    public long FileSizeInBytes { get; } = fileSizeInBytes;

    [JsonPropertyName("file-footer-size-in-bytes")]
    public long FileFooterSizeInBytes { get; } = fileFooterSizeInBytes;

    [JsonPropertyName("blob-metadata")] public ICollection<BlobMetadata> BlobMetadata { get; } = blobMetadata;
}