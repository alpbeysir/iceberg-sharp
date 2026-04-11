using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class PartitionStatisticsFile(long fileSizeInBytes, long snapshotId, string statisticsPath)
{
    [JsonPropertyName("snapshot-id")] public long SnapshotId { get; } = snapshotId;

    [JsonPropertyName("statistics-path")] public string StatisticsPath { get; } = statisticsPath;

    [JsonPropertyName("file-size-in-bytes")]
    public long FileSizeInBytes { get; } = fileSizeInBytes;
}