using System.Collections.Immutable;
using EngineeredWood.Expressions;

namespace Iceberg.Net.Metadata;

public enum DataFileContent
{
    Data,
    PositionDeletes,
    EqualityDeletes
}

public readonly record struct DataFile
{
    public required DataFileContent Content { get; init; }
    public ImmutableArray<int>? EqualityIds { get; init; }
    public string? ReferencedDataFile { get; init; }
    public required string FilePath { get; init; }
    public required string FileFormat { get; init; }
    public ImmutableArray<LiteralValue?> Partition { get; init; }
    public required long RecordCount { get; init; }
    public required long FileSizeInBytes { get; init; }
    public ImmutableDictionary<int, long>? ColumnSizes { get; init; }
    public ImmutableDictionary<int, long>? ValueCounts { get; init; }
    public ImmutableDictionary<int, long>? NullValueCounts { get; init; }
    public ImmutableDictionary<int, long>? NanValueCounts { get; init; }
    public ImmutableDictionary<int, LiteralValue>? LowerBounds { get; init; }
    public ImmutableDictionary<int, LiteralValue>? UpperBounds { get; init; }
    public byte[]? KeyMetadata { get; init; }
    public ImmutableArray<long>? SplitOffsets { get; init; }
    public int? SortOrderId { get; init; }
}
