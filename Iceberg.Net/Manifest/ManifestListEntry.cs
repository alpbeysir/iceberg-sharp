using System.Collections.Immutable;

namespace Iceberg.Net.Metadata;

public enum Content
{
    Data,
    Deletes
}

public readonly record struct ManifestListEntry
{
    public required Content Content { get; init; }
    public required long SequenceNumber { get; init; }
    public required long MinSequenceNumber { get; init; }
    public required int AddedFilesCount { get; init; }
    public required int ExistingFilesCount { get; init; }
    public required int DeletedFilesCount { get; init; }
    public required long AddedRowsCount { get; init; }
    public required long ExistingRowsCount { get; init; }
    public required long DeletedRowsCount { get; init; }
    public required string ManifestPath { get; init; }
    public required long ManifestLength { get; init; }
    public required int PartitionSpecId { get; init; }
    public required long AddedSnapshotId { get; init; }
    public ImmutableArray<FieldSummary>? Partitions { get; init; }
    public byte[]? KeyMetadata { get; init; }

    public static string GetFileName(long snapshotId, long sequenceNumber, Guid guid)
    {
        return $"snap-{snapshotId}-{sequenceNumber}-{guid}.avro";
    }
}
