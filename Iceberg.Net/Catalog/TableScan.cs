using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using EngineeredWood.IO;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Schemas;
using Iceberg.Net.Storage;
using Microsoft.Extensions.Logging;

namespace Iceberg.Net.Catalog;

public sealed class TableScan(Table table)
{
    private readonly ILogger<TableScan> _logger = table.Catalog.LoggerFactory.CreateLogger<TableScan>();

    public IEnumerable<TRow> ReadRows<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
        TRow>(
        long? snapshotId = null) where TRow : IArrowSerializer<TRow>
    {
        Snapshot snapshot = GetSnapshotOrLatest(snapshotId);
        Schemas.Schema snapshotSchema = GetSnapshotSchema(snapshot);
        VerifyRowSchema<TRow>(snapshot, snapshotSchema);

        Channel<RecordBatch> columnBuffers = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(16384)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        Task read = ReadArrow(snapshot.SnapshotId, columnBuffers.Writer);
        _ = read.ContinueWith(
            completed => columnBuffers.Writer.TryComplete(completed.Exception?.GetBaseException()),
            TaskScheduler.Default);

        long rowCount = 0;
        foreach (RecordBatch batch in columnBuffers.Reader.ReadAllAsync().ToBlockingEnumerable())
        {
            IReadOnlyList<TRow> rows = TRow.ListFromRecordBatch(batch);
            foreach (TRow row in rows)
            {
                rowCount++;
                yield return row;
            }
            batch.Dispose();
        }

        read.GetAwaiter().GetResult();

        _logger.LogInformation(
            "Completed scan of table {TableIdentifier} with {RowCount} rows",
            table.Identifier.ToString(),
            rowCount);
    }

    private async Task ReadArrow(
        long? snapshotId,
        ChannelWriter<RecordBatch> results,
        CancellationToken cancellationToken = default)
    {
        Channel<ManifestEntry> manifestEntries = Channel.CreateBounded<ManifestEntry>(
            new BoundedChannelOptions(8192)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        Task manifestRead = ReadManifestEntries(
            manifestEntries.Writer,
            snapshotId,
            cancellationToken);

        Task dataFileReaders = Parallel.ForEachAsync(
            manifestEntries.Reader.ReadAllAsync(cancellationToken),
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 16
            },
            async (entry, token) => { await ReadDataFileAsync(entry.DataFile, results, token); });

        await manifestRead;
        manifestEntries.Writer.Complete();

        await dataFileReaders;
    }

    public async Task ReadManifestEntries(
        ChannelWriter<ManifestEntry> results,
        long? snapshotId = null,
        CancellationToken cancellationToken = default)
    {
        Snapshot snapshot = GetSnapshotOrLatest(snapshotId);
        _logger.LogInformation(
            "Scanning table {TableIdentifier} at snapshot {SnapshotId}",
            table.Identifier.ToString(),
            snapshot.SnapshotId);

        Channel<ManifestListEntry> manifestListEntries = Channel.CreateBounded<ManifestListEntry>(
            new BoundedChannelOptions(8192)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true
            });

        Task snapshotRead = ReadSnapshotAsync(
            snapshot.SnapshotId,
            manifestListEntries.Writer,
            cancellationToken);

        Task manifestReaders = Parallel.ForEachAsync(
            manifestListEntries.Reader.ReadAllAsync(cancellationToken),
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 4
            },
            async (entry, token) => { await ReadManifestAsync(entry, results, token); });

        await snapshotRead;
        manifestListEntries.Writer.Complete();
        await manifestReaders;
        _logger.LogDebug(
            "Completed manifest scan for table {TableIdentifier} at snapshot {SnapshotId}",
            table.Identifier.ToString(),
            snapshot.SnapshotId);
    }

    internal async Task ReadSnapshotAsync(
        long snapshotId,
        ChannelWriter<ManifestListEntry> results,
        CancellationToken cancellationToken = default)
    {
        Snapshot snapshot = table.Metadata.SnapshotsById[snapshotId];
        _logger.LogDebug(
            "Reading manifest list {ManifestListPath} for snapshot {SnapshotId}",
            snapshot.ManifestList,
            snapshotId);
        Schemas.Schema schema = GetSnapshotSchema(snapshot);

        await using PathAndFile<IRandomAccessFile> manifestListFile = await OpenFile(
            snapshot.ManifestList,
            cancellationToken);
        await using RandomAccessFileStream manifestListStream = new(manifestListFile.File);
        await ManifestIO.ReadManifestListAsync(
            manifestListStream,
            results,
            schema,
            table.Metadata.PartitionSpecs,
            cancellationToken);
    }

    private async Task ReadManifestAsync(
        ManifestListEntry manifestListEntry,
        ChannelWriter<ManifestEntry> results,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Reading manifest {ManifestPath} for table {TableIdentifier}",
            manifestListEntry.ManifestPath,
            table.Identifier.ToString());
        await using PathAndFile<IRandomAccessFile> manifestFile = await OpenFile(
            manifestListEntry.ManifestPath,
            cancellationToken);
        await using RandomAccessFileStream manifestStream = new(manifestFile.File);
        await ManifestIO.ReadManifestAsync(
            manifestStream,
            results,
            entry =>
            {
                // TODO only inherit if status = added
                return entry with
                {
                    FileSequenceNumber = entry.FileSequenceNumber ?? manifestListEntry.SequenceNumber,
                    SequenceNumber = entry.SequenceNumber ?? manifestListEntry.SequenceNumber,
                    SnapshotId = entry.SnapshotId ?? manifestListEntry.AddedSnapshotId
                };
            },
            cancellationToken);
    }

    private async Task ReadDataFileAsync(
        DataFile dataFile,
        ChannelWriter<RecordBatch> results,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Reading data file {DataFilePath} for table {TableIdentifier}",
            dataFile.FilePath,
            table.Identifier.ToString());
        await using PathAndFile<IRandomAccessFile> storageFile = await OpenFile(
            dataFile.FilePath,
            cancellationToken);
        await using RandomAccessFileStream dataFileStream = new(storageFile.File);
        IDataFileFormat dataFileFormat = DataFileFormatRegistry.Resolve(
            dataFile.FileFormat,
            table.Properties);
        await dataFileFormat.ReadAsync(
            dataFileStream,
            results,
            cancellationToken);
    }

    private async Task<PathAndFile<IRandomAccessFile>> OpenFile(
        string path,
        CancellationToken cancellationToken = default)
    {
        if (!Uri.TryCreate(path, UriKind.RelativeOrAbsolute, out Uri? uri))
            throw new ArgumentException($"Invalid URI: {path}");

        IRandomAccessFile file = await table.ReadFile(uri, cancellationToken);
        return new PathAndFile<IRandomAccessFile>(uri, file);
    }

    private Snapshot GetSnapshotOrLatest(long? snapshotId)
    {
        if (snapshotId is not null)
            return table.Metadata.SnapshotsById.TryGetValue(snapshotId.Value, out Snapshot? result)
                ? result
                : throw new ArgumentOutOfRangeException(nameof(snapshotId));

        long currentSnapshotId = table.Metadata.CurrentSnapshotId ??
                                 throw new InvalidOperationException("Table doesn't have any snapshots");
        return table.Metadata.SnapshotsById.TryGetValue(currentSnapshotId, out Snapshot? currentSnapshot)
            ? currentSnapshot
            : throw new UnreachableException("Could not find the current snapshot");
    }

    private Schemas.Schema GetSnapshotSchema(Snapshot snapshot)
    {
        int schemaId = snapshot.SchemaId ?? table.Metadata.CurrentSchemaId ??
            throw new InvalidDataException("Table metadata does not identify a schema for the snapshot.");
        return table.Metadata.SchemasById.TryGetValue(schemaId, out Schemas.Schema? schema)
            ? schema
            : throw new InvalidDataException(
                $"Snapshot {snapshot.SnapshotId} refers to unknown schema ID {schemaId}.");
    }

    private static void VerifyRowSchema<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
        TRow>(Snapshot snapshot, Schemas.Schema snapshotSchema)
    {
        IReadOnlyDictionary<string, int> fieldIds = SchemaUtilities.FieldIdsByPath(snapshotSchema);
        Schemas.Schema requestedSchema = CSharpSchemas.ToIcebergSchema(
            typeof(TRow),
            snapshotSchema.SchemaId,
            path => fieldIds.TryGetValue(path, out int fieldId)
                ? fieldId
                : throw new InvalidOperationException(
                    $"Requested row type '{typeof(TRow).FullName}' contains field '{path}' that is not present " +
                    $"in schema {snapshotSchema.SchemaId} of snapshot {snapshot.SnapshotId}."));

        if (!new IcebergTypeComparer().Equals(requestedSchema, snapshotSchema))
            throw new InvalidOperationException(
                $"Requested row type '{typeof(TRow).FullName}' does not match schema " +
                $"{snapshotSchema.SchemaId} of snapshot {snapshot.SnapshotId}.");
    }
}
