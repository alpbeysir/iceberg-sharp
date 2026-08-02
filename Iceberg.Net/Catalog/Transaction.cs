using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Serialization;
using Avro.File;
using Avro.Generic;
using Iceberg.Net.Metadata;
using Iceberg.Net.Misc;
using Iceberg.Net.Query;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Schemas;
using Iceberg.Net.Storage;
using ParquetSharp;
using ParquetSharp.Arrow;
using Schema = Iceberg.Net.Schemas.Schema;
using SortOrder = Iceberg.Net.Metadata.SortOrder;

namespace Iceberg.Net.Catalog;

public sealed class Transaction(Table table, bool commitOnDispose = false) : IAsyncDisposable
{
    private readonly List<ITableRequirement> _requirements = [];
    private readonly List<ITableUpdate> _updates = [];

    private Table Table { get; set; } = table;

    public async ValueTask DisposeAsync()
    {
        if (commitOnDispose)
            await Commit();
    }

    private IEnumerable<PathAndStream> AllFiles(long? snapshotId)
    {
        Snapshot snapshot = GetSnapshotOrLatest(snapshotId);
        PathAndStream manifestListFile = OpenFile(snapshot.ManifestList).GetAwaiter().GetResult();
        using IFileReader<ManifestListEntry> manifestListReader = ManifestListEntry.GetReader(manifestListFile.Stream);
        foreach (ManifestListEntry manifestListEntry in manifestListReader.NextEntries)
        {
            PathAndStream manifestFile = OpenFile(manifestListEntry.ManifestPath).GetAwaiter().GetResult();
            using IFileReader<ManifestEntry> manifestReader = ManifestEntry.GetReader(manifestFile.Stream);
            foreach (ManifestEntry manifestEntry in manifestReader.NextEntries)
                yield return OpenFile(manifestEntry.DataFile.FilePath).GetAwaiter().GetResult();
        }
    }

    public IEnumerable<TRow> ReadRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)] TRow>(
        long? snapshotId = null) where TRow : IArrowSerializer<TRow>
    {
        Channel<RecordBatch> columnBuffers = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(16384)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        ReadArrow(snapshotId, columnBuffers).ContinueWith(_ => columnBuffers.Writer.TryComplete());

        foreach (RecordBatch batch in columnBuffers.Reader.ReadAllAsync().ToBlockingEnumerable())
        {
            IReadOnlyList<TRow> rows = TRow.ListFromRecordBatch(batch);
            foreach (TRow row in rows) yield return row;
            batch.Dispose();
        }
    }

    public async Task ReadArrow(
        long? snapshotId,
        Channel<RecordBatch> results,
        CancellationToken cancellationToken = default)
    {
        if (!Table.IsLoaded) throw new InvalidOperationException("Cannot read uninitialized table");

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

        Task snapshotRead = ReadSnapshotAsync(snapshot.SnapshotId, manifestListEntries, cancellationToken);

        Task manifestReaders = Parallel.ForEachAsync(
            manifestListEntries.Reader.ReadAllAsync(cancellationToken),
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 4
            },
            async (entry, token) => { await ReadManifestAsync(entry, manifestEntries, token); });

        Task dataFileReaders = Parallel.ForEachAsync(
            manifestEntries.Reader.ReadAllAsync(cancellationToken),
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 16
            },
            async (entry, token) => { await ReadDataFileAsync(entry.DataFile, results, token); });

        await snapshotRead;
        manifestListEntries.Writer.Complete();

        await manifestReaders;
        manifestEntries.Writer.Complete();

        await dataFileReaders;
    }

    public async Task AppendRowsAot<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)] TRow>(
        IEnumerable<TRow> rows,
        CancellationToken cancellationToken = default) where TRow : IArrowSerializer<TRow>
    {
        var schemaId = Table.Metadata?.CurrentSchemaId ?? 0;
        var nextFieldId = (Table.Metadata?.LastColumnId ?? 0) + 1;
        Schema schema = CSharpSchema.ToIcebergSchema(typeof(TRow), schemaId, _ => nextFieldId++);

        Channel<RecordBatch> channel = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(2048)
            {
                SingleWriter = true
            });

        Task convertToArrow = Task.Run(
            async () =>
            {
                foreach (TRow[] chunk in rows.Chunk(16384))
                {
                    RecordBatch batch = TRow.ToRecordBatch(chunk);
                    await channel.Writer.WriteAsync(batch, cancellationToken);
                }
            },
            cancellationToken);

        Task append = AppendArrow(channel, schema, cancellationToken);

        await convertToArrow;
        channel.Writer.TryComplete();
        await append;
    }

    public async Task AppendRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)] TRow>(
        IEnumerable<TRow> rows,
        CancellationToken cancellationToken = default)
    {
        var schemaId = Table.Metadata?.CurrentSchemaId ?? 0;
        var nextFieldId = (Table.Metadata?.LastColumnId ?? 0) + 1;
        Schema schema = CSharpSchema.ToIcebergSchema(typeof(TRow), schemaId, _ => nextFieldId++);

        Channel<RecordBatch> channel = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(2048)
            {
                SingleWriter = true
            });

        Task convertToArrow = Task.Run(
            async () =>
            {
                foreach (TRow[] chunk in rows.Chunk(16384))
                {
                    RecordBatch batch = RecordBatchBuilder.FromObjects(chunk);
                    await channel.Writer.WriteAsync(batch, cancellationToken);
                }
            },
            cancellationToken);

        Task append = AppendArrow(channel, schema, cancellationToken);

        await convertToArrow;
        channel.Writer.TryComplete();
        await append;
    }

    internal async Task AppendArrow(
        Channel<RecordBatch> data,
        Schema schema,
        CancellationToken cancellationToken = default)
    {
        PartitionSpec partitionSpec = new([], 0);
        SortOrder sortOrder = new([], 0);

        await EnsureTableInitialized(schema, partitionSpec, sortOrder, cancellationToken);

        schema = GetSchema();

        Channel<DataFileWriteResult> dataFiles = Channel.CreateBounded<DataFileWriteResult>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        Task dataFileWrite = WriteDataFileAsync(
            data,
            schema,
            dataFiles,
            cancellationToken);

        var snapshotId = Utils.GenerateSnapshotId();

        Channel<ManifestListEntry> existingManifests = Channel.CreateBounded<ManifestListEntry>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        Task existingSnapshotRead = Task.CompletedTask;
        if (Table.Metadata!.CurrentSnapshotId > 0)
            existingSnapshotRead = ReadSnapshotAsync(
                Table.Metadata!.CurrentSnapshotId.Value,
                existingManifests,
                cancellationToken);

        Channel<ManifestFileWriteResult> newManifests = Channel.CreateBounded<ManifestFileWriteResult>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        // TODO allow writing multiple manifests for scaling
        Task manifestWrite = WriteManifestAsync(
            snapshotId,
            schema,
            partitionSpec,
            dataFiles,
            newManifests,
            cancellationToken);

        Task<Snapshot> snapshotCreate = CreateSnapshotAsync(
            snapshotId,
            null,
            existingManifests,
            newManifests,
            schema.SchemaId!.Value,
            cancellationToken);

        await dataFileWrite;
        dataFiles.Writer.Complete();

        await manifestWrite;
        newManifests.Writer.Complete();

        await existingSnapshotRead;
        existingManifests.Writer.Complete();

        Snapshot snapshot = await snapshotCreate;

        StageChanges(
            [],
            [
                new SetCurrentSchemaTableUpdate((int)schema.SchemaId!),
                new AddSnapshotTableUpdate(snapshot),
                new SetSnapshotRefTableUpdate(
                    Utils.InitialBranch,
                    SnapshotReferenceType.Branch,
                    snapshotId,
                    null,
                    null,
                    null)
            ]);
    }

    private async ValueTask ReadDataFileAsync(
        DataFile dataFile,
        Channel<RecordBatch> results,
        CancellationToken cancellationToken)
    {
        await using PathAndStream dataFileStream = await OpenFile(dataFile.FilePath, cancellationToken);

        using ArrowReaderProperties arrowReaderProperties = ArrowReaderProperties.GetDefault();
        using ReaderProperties parquetReaderProperties = ReaderProperties.GetDefaultReaderProperties();
        using FileReader arrowReader = new(
            dataFileStream.Stream,
            parquetReaderProperties,
            arrowReaderProperties,
            true);
        using IArrowArrayStream recordBatchReader = arrowReader.GetRecordBatchReader();

        while (!cancellationToken.IsCancellationRequested)
        {
            // TODO exceptions are swallowed
            RecordBatch? batch = await recordBatchReader.ReadNextRecordBatchAsync(cancellationToken);
            if (batch is null) break;
            await results.Writer.WriteAsync(batch, cancellationToken);
        }
    }

    private async Task WriteDataFileAsync(
        Channel<RecordBatch> batches,
        Schema schema,
        Channel<DataFileWriteResult> results,
        CancellationToken cancellationToken)
    {
        await using PathAndStream dataFile = await NewDataFile(cancellationToken);
        using ArrowWriterProperties arrowWriterProperties = ArrowWriterProperties.GetDefault();
        using WriterPropertiesBuilder parquetWriterPropertiesBuilder = new();
        parquetWriterPropertiesBuilder.Compression(Compression.Zstd);
        parquetWriterPropertiesBuilder.MaxRowGroupLength(256 * 1024);
        parquetWriterPropertiesBuilder.EnableStoreDecimalAsInteger();
        Apache.Arrow.Schema arrowSchema = ArrowSchema.FromSchema(schema);
        using FileWriter arrowWriter = new(
            dataFile.Stream,
            arrowSchema,
            parquetWriterPropertiesBuilder.Build(),
            arrowWriterProperties,
            true);

        long written = 0;

        await foreach (RecordBatch batch in batches.Reader.ReadAllAsync(cancellationToken))
        {
            // TODO for now have to clone due to arrow limitations
            arrowWriter.WriteBufferedRecordBatch(batch.Clone());
            written += batch.Length;
            batch.Dispose();
        }

        arrowWriter.Close();

        await results.Writer.WriteAsync(
            new DataFileWriteResult(
                dataFile.Path,
                written,
                dataFile.Stream.Length),
            cancellationToken);
    }

    private async Task ReadSnapshotAsync(
        long snapshotId,
        Channel<ManifestListEntry> results,
        CancellationToken cancellationToken = default)
    {
        Snapshot snapshot = Table.Metadata!.SnapshotsById[snapshotId];

        PathAndStream manifestListFile = await OpenFile(snapshot.ManifestList, cancellationToken);

        using IFileReader<ManifestListEntry>
            manifestListAppender = ManifestListEntry.GetReader(manifestListFile.Stream);

        foreach (ManifestListEntry entry in manifestListAppender.NextEntries)
            await results.Writer.WriteAsync(entry, cancellationToken);

        await manifestListFile.DisposeAsync();
    }

    private async Task<Snapshot> CreateSnapshotAsync(
        long snapshotId,
        long? parentSnapshotId,
        Channel<ManifestListEntry> existingEntries,
        Channel<ManifestFileWriteResult> newEntries,
        int schemaId,
        CancellationToken cancellationToken = default)
    {
        var sequenceNumber = (long)Table.Metadata!.LastSequenceNumber! + 1;
        Summary summary = new() { Operation = SummaryOperation.Overwrite };

        PathAndStream manifestListFile = await CreateManifestListFile(snapshotId, sequenceNumber, cancellationToken);

        using IFileWriter<ManifestListEntry> manifestListAppender = ManifestListEntry.GetAppender(
            manifestListFile.Stream,
            snapshotId,
            null,
            sequenceNumber);

        await foreach (ManifestFileWriteResult entry in newEntries.Reader.ReadAllAsync(cancellationToken))
        {
            summary.AddedDataFiles += entry.AddedFileCount;
            summary.AddedRecords += entry.AddedRowsCount;
            summary.AddedFilesSize += entry.AddedFilesSize;

            ManifestListEntry manifestListEntry = new()
            {
                ManifestPath = entry.Location.AbsoluteUri,
                ManifestLength = entry.FileSize,
                PartitionSpecId = 0,
                Content = Content.Data,
                SequenceNumber = sequenceNumber,
                MinSequenceNumber = sequenceNumber,
                AddedSnapshotId = snapshotId,
                AddedFilesCount = entry.AddedFileCount,
                ExistingFilesCount = 0,
                DeletedFilesCount = 0,
                AddedRowsCount = entry.AddedRowsCount,
                ExistingRowsCount = 0,
                DeletedRowsCount = 0
            };
            manifestListAppender.Append(manifestListEntry);
            manifestListAppender.Flush();
        }

        await foreach (ManifestListEntry entry in existingEntries.Reader.ReadAllAsync(cancellationToken))
        {
            manifestListAppender.Append(entry);
            manifestListAppender.Flush();
        }

        await manifestListFile.DisposeAsync();

        return new Snapshot
        {
            SnapshotId = snapshotId,
            ParentSnapshotId = parentSnapshotId,
            SequenceNumber = sequenceNumber,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ManifestList = manifestListFile.Path.AbsoluteUri,
            FirstRowId = null,
            AddedRows = summary.AddedRecords,
            Summary = summary,
            SchemaId = schemaId
        };
    }

    private async ValueTask ReadManifestAsync(
        ManifestListEntry manifestListEntry,
        Channel<ManifestEntry> results,
        CancellationToken cancellationToken)
    {
        PathAndStream manifestFile = await OpenFile(manifestListEntry.ManifestPath, cancellationToken);

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

            await results.Writer.WriteAsync(inheritedEntry, cancellationToken);
        }

        await manifestFile.DisposeAsync();
    }

    private async Task WriteManifestAsync(
        long snapshotId,
        Schema schema,
        PartitionSpec partitionSpec,
        Channel<DataFileWriteResult> dataFiles,
        Channel<ManifestFileWriteResult> results,
        CancellationToken cancellationToken)
    {
        PathAndStream manifestFile = await CreateManifestFile(cancellationToken);

        using IFileWriter<ManifestEntry> manifestAppender = ManifestEntry.GetAppender(
            manifestFile.Stream,
            schema,
            partitionSpec,
            Content.Data);

        long addedFilesSize = 0;
        long addedRowsCount = 0;
        var addedDataFilesCount = 0;

        await foreach (DataFileWriteResult entry in dataFiles.Reader.ReadAllAsync(cancellationToken))
        {
            addedRowsCount += entry.RecordCount;
            addedDataFilesCount++;
            addedFilesSize += entry.FileSize;
            ManifestEntry manifestEntry = new()
            {
                Status = Status.Added,
                SnapshotId = snapshotId,
                SequenceNumber = null,
                FileSequenceNumber = null,
                DataFile = new DataFile
                {
                    Content = DataFileContent.Data,
                    FilePath = entry.Location.AbsoluteUri,
                    FileFormat = "parquet",
                    Partition = new GenericRecord(Utils.EmptyPartitionAvroSchema),
                    RecordCount = entry.RecordCount,
                    FileSizeInBytes = entry.FileSize
                }
            };
            manifestAppender.Append(manifestEntry);
            manifestAppender.Flush();
        }

        await results.Writer.WriteAsync(
            new ManifestFileWriteResult(
                manifestFile.Path,
                manifestFile.Stream.Length,
                addedRowsCount,
                addedDataFilesCount,
                addedFilesSize),
            cancellationToken);

        await manifestFile.DisposeAsync();
    }

    private async ValueTask<PathAndStream> NewDataFile(CancellationToken cancellationToken = default)
    {
        Uri parquetFilePath = new(Table.DataFolderUri, Utils.GetParquetFileName(0, 0, Guid.NewGuid()));
        Stream parquetStream = await Table.ObjectStorage.Open(
            parquetFilePath,
            FileMode.CreateNew,
            cancellationToken);
        return new PathAndStream(parquetFilePath, parquetStream);
    }

    private async ValueTask<PathAndStream> CreateManifestFile(CancellationToken cancellationToken = default)
    {
        Uri manifestFilePath = new(
            Table.MetadataFolderUri,
            ManifestEntry.GetFileName(Guid.NewGuid(), 0));
        Stream stream = await Table.ObjectStorage.Open(
            manifestFilePath,
            FileMode.CreateNew,
            cancellationToken);
        return new PathAndStream(manifestFilePath, stream);
    }

    private async ValueTask<PathAndStream> CreateManifestListFile(
        long snapshotId,
        long sequenceNumber,
        CancellationToken cancellationToken = default)
    {
        Uri manifestListFilePath = new(
            Table.MetadataFolderUri,
            ManifestListEntry.GetFileName(snapshotId, sequenceNumber, Guid.NewGuid()));
        Stream manifestListStream = await Table.ObjectStorage.Open(
            manifestListFilePath,
            FileMode.CreateNew,
            cancellationToken);
        return new PathAndStream(manifestListFilePath, manifestListStream);
    }

    private async Task<PathAndStream> OpenFile(
        string path,
        CancellationToken cancellationToken = default)
    {
        var success = Uri.TryCreate(path, UriKind.RelativeOrAbsolute, out Uri? uri);
        if (!success) throw new ArgumentException($"Invalid URI: {path}");

        Stream stream = await Table.ObjectStorage.Open(
            uri!,
            FileMode.Open,
            cancellationToken);
        return new PathAndStream(uri!, stream);
    }

    private async Task EnsureTableInitialized(
        Schema schema,
        PartitionSpec partitionSpec,
        SortOrder sortOrder,
        CancellationToken cancellationToken = default)
    {
        if (!Table.IsLoaded)
            try
            {
                Table =
                    await Table.Catalog.LoadTableAsync(Table.Identifier, cancellationToken: cancellationToken);
            }
            catch (Exception)
            {
                Table = await Table.Catalog.CreateTableInternalAsync(
                    Table.Identifier,
                    schema,
                    true,
                    cancellationToken);
                StageChanges(
                    [new AssertCreate()],
                    [
                        new SetLocationTableUpdate(Table.Metadata!.Location),
                        new AddSchemaTableUpdate(Table.Metadata!.Schemas[0]),
                        new AddPartitionSpecTableUpdate(partitionSpec),
                        new AddSortOrderTableUpdate(sortOrder)
                    ]);
            }
    }

    public async Task Commit(CancellationToken cancellationToken = default)
    {
        // nothing to commit
        if (_updates.Count == 0) return;

        // TODO retry (could also be handled in catalog)
        Table response = await Table.Catalog.UpdateTableAsync(
            Table.Identifier,
            _updates,
            _requirements,
            cancellationToken);
        _updates.Clear();
        _requirements.Clear();

        Table = response;
    }

    private void StageChanges(List<ITableRequirement> requirements, List<ITableUpdate> updates)
    {
        Table.Metadata!.Apply(updates);
        _requirements.AddRange(requirements);
        _updates.AddRange(updates);
    }

    private Snapshot GetSnapshotOrLatest(long? snapshotId)
    {
        if (snapshotId is not null)
        {
            return Table.Metadata!.SnapshotsById.TryGetValue(snapshotId.Value, out Snapshot? result)
                ? result
                : throw new ArgumentOutOfRangeException(nameof(snapshotId));
        }
        else
        {
            var currentSnapshotId = Table.Metadata!.CurrentSnapshotId ??
                                    throw new InvalidOperationException("Table doesn't have any snapshots");
            return Table.Metadata.SnapshotsById.TryGetValue(currentSnapshotId, out Snapshot? result)
                ? result
                : throw new UnreachableException("Could not find the current snapshot");
        }
    }

    private Schema GetSchema()
    {
        return Table.Metadata!.SchemasById[Table.Metadata!.CurrentSchemaId!.Value];
    }
}
