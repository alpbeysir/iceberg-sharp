using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class DataFile(
    CountMap columnSizes,
    string content,
    FileFormat fileFormat,
    string filePath,
    long fileSizeInBytes,
    long? firstRowId,
    string keyMetadata,
    ValueMap lowerBounds,
    CountMap nanValueCounts,
    CountMap nullValueCounts,
    ICollection<bool> partition,
    long recordCount,
    int? sortOrderId,
    int specId,
    ICollection<long> splitOffsets,
    ValueMap upperBounds,
    CountMap valueCounts)
    : ContentFile(fileFormat, filePath, fileSizeInBytes, keyMetadata, partition, recordCount, sortOrderId, specId,
        splitOffsets)
{
    [JsonPropertyName("content")] public string Content { get; } = content;

    /// <summary>
    ///     The first row ID assigned to the first row in the data file
    /// </summary>
    [JsonPropertyName("first-row-id")]
    public long? FirstRowId { get; } = firstRowId;

    /// <summary>
    ///     Map of column id to total count, including null and NaN
    /// </summary>
    [JsonPropertyName("column-sizes")]
    public CountMap ColumnSizes { get; } = columnSizes;

    /// <summary>
    ///     Map of column id to null value count
    /// </summary>
    [JsonPropertyName("value-counts")]
    public CountMap ValueCounts { get; } = valueCounts;

    /// <summary>
    ///     Map of column id to null value count
    /// </summary>
    [JsonPropertyName("null-value-counts")]
    public CountMap NullValueCounts { get; } = nullValueCounts;

    /// <summary>
    ///     Map of column id to number of NaN values in the column
    /// </summary>
    [JsonPropertyName("nan-value-counts")]
    public CountMap NanValueCounts { get; } = nanValueCounts;

    /// <summary>
    ///     Map of column id to lower bound primitive type values
    /// </summary>
    [JsonPropertyName("lower-bounds")]
    public ValueMap LowerBounds { get; } = lowerBounds;

    /// <summary>
    ///     Map of column id to upper bound primitive type values
    /// </summary>
    [JsonPropertyName("upper-bounds")]
    public ValueMap UpperBounds { get; } = upperBounds;
}