using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Avro.File;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Catalog;

public sealed class TableScan(Table table)
{
    public IEnumerable<TRow> ReadRows<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)] TRow>(
        long? snapshotId = null) where TRow : IArrowSerializer<TRow>
    {
        Channel<RecordBatch> columnBuffers = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(16384)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        ReadArrow(snapshotId, columnBuffers.Writer)
            .ContinueWith(_ => columnBuffers.Writer.TryComplete());

        foreach (RecordBatch batch in columnBuffers.Reader.ReadAllAsync().ToBlockingEnumerable())
        {
            IReadOnlyList<TRow> rows = TRow.ListFromRecordBatch(batch);
            foreach (TRow row in rows) yield return row;
            batch.Dispose();
        }
    }

    public async Task ReadArrow(
        long? snapshotId,
        ChannelWriter<RecordBatch> results,
        CancellationToken cancellationToken = default)
    {
        if (!table.IsLoaded) throw new InvalidOperationException("Cannot read uninitialized table");

        Snapshot snapshot = GetSnapshotOrLatest(snapshotId);

        Channel<ManifestListEntry> manifestListEntries = Channel.CreateBounded<ManifestListEntry>(
            new BoundedChannelOptions(8192)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true
            });

        Channel<ManifestEntry> manifestEntries = Channel.CreateBounded<ManifestEntry>(
            new BoundedChannelOptions(8192)
            {
                FullMode = BoundedChannelFullMode.Wait
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
            async (entry, token) =>
            {
                await ReadManifestAsync(entry, manifestEntries.Writer, token);
            });

        Task dataFileReaders = Parallel.ForEachAsync(
            manifestEntries.Reader.ReadAllAsync(cancellationToken),
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 16
            },
            async (entry, token) =>
            {
                await ReadDataFileAsync(entry.DataFile, results, token);
            });

        await snapshotRead;
        manifestListEntries.Writer.Complete();

        await manifestReaders;
        manifestEntries.Writer.Complete();

        await dataFileReaders;
    }

    internal async Task ReadSnapshotAsync(
        long snapshotId,
        ChannelWriter<ManifestListEntry> results,
        CancellationToken cancellationToken = default)
    {
        Snapshot snapshot = table.Metadata!.SnapshotsById[snapshotId];

        await using PathAndStream manifestListFile = await OpenFile(
            snapshot.ManifestList,
            cancellationToken);
        using IFileReader<ManifestListEntry> manifestListReader =
            ManifestListEntry.GetReader(manifestListFile.Stream);

        foreach (ManifestListEntry entry in manifestListReader.NextEntries)
            await results.WriteAsync(entry, cancellationToken);
    }

    private async Task ReadManifestAsync(
        ManifestListEntry manifestListEntry,
        ChannelWriter<ManifestEntry> results,
        CancellationToken cancellationToken)
    {
        await using PathAndStream manifestFile = await OpenFile(
            manifestListEntry.ManifestPath,
            cancellationToken);
        using IFileReader<ManifestEntry> manifestReader = ManifestEntry.GetReader(manifestFile.Stream);

        foreach (ManifestEntry entry in manifestReader.NextEntries)
        {
            // TODO only inherit if status = added
            ManifestEntry inheritedEntry = entry with
            {
                FileSequenceNumber = entry.FileSequenceNumber ?? manifestListEntry.SequenceNumber,
                SequenceNumber = entry.SequenceNumber ?? manifestListEntry.SequenceNumber,
                SnapshotId = entry.SnapshotId ?? manifestListEntry.AddedSnapshotId
            };

            await results.WriteAsync(inheritedEntry, cancellationToken);
        }
    }

    private async Task ReadDataFileAsync(
        DataFile dataFile,
        ChannelWriter<RecordBatch> results,
        CancellationToken cancellationToken)
    {
        await using PathAndStream dataFileStream = await OpenFile(
            dataFile.FilePath,
            cancellationToken);
        IDataFileFormat dataFileFormat = DataFileFormatRegistry.Resolve(
            dataFile.FileFormat,
            table.Properties);
        await dataFileFormat.ReadAsync(
            dataFileStream.Stream,
            results,
            cancellationToken);
    }

    private async Task<PathAndStream> OpenFile(
        string path,
        CancellationToken cancellationToken = default)
    {
        if (!Uri.TryCreate(path, UriKind.RelativeOrAbsolute, out Uri? uri))
            throw new ArgumentException($"Invalid URI: {path}");

        Stream stream = await table.Open(uri, FileMode.Open, cancellationToken);
        return new PathAndStream(uri, stream);
    }

    private Snapshot GetSnapshotOrLatest(long? snapshotId)
    {
        if (snapshotId is not null)
            return table.Metadata!.SnapshotsById.TryGetValue(snapshotId.Value, out Snapshot? result)
                ? result
                : throw new ArgumentOutOfRangeException(nameof(snapshotId));

        long currentSnapshotId = table.Metadata!.CurrentSnapshotId ??
                                 throw new InvalidOperationException("Table doesn't have any snapshots");
        return table.Metadata.SnapshotsById.TryGetValue(currentSnapshotId, out Snapshot? currentSnapshot)
            ? currentSnapshot
            : throw new UnreachableException("Could not find the current snapshot");
    }
}
