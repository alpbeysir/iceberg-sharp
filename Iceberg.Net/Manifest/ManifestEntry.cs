namespace Iceberg.Net.Metadata;

public enum Status
{
    Existing = 0,
    Added = 1,
    Deleted = 2
}

public readonly record struct ManifestEntry
{
    public long? SequenceNumber { get; init; }
    public long? FileSequenceNumber { get; init; }
    public required Status Status { get; init; }
    public long? SnapshotId { get; init; }
    public required DataFile DataFile { get; init; }

    public static string GetFileName(Guid guid, int sequence)
    {
        return $"{guid}-m{sequence}.avro";
    }
}
