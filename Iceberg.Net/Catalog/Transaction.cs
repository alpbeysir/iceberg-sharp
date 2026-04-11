using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Serialization;
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

    public async Task<IQueryable<TRow>> ReadQueryable<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.AllFields | DynamicallyAccessedMemberTypes.AllProperties |
            DynamicallyAccessedMemberTypes.AllNestedTypes)]
        TRow>(
        long? snapshotId = null) where TRow : IArrowSerializer<TRow>
    {
        var provider = new IcebergQueryProvider<TRow>(this);
        return new IcebergQueryable<TRow>(provider, null);
    }

    private IEnumerable<PathAndStream> AllFiles(long? snapshotId)
    {
        var snapshot = GetSnapshotOrLatest(snapshotId);
        var manifestListFile = OpenFile(snapshot.ManifestList).GetAwaiter().GetResult();
        using var manifestListReader = ManifestListEntry.GetReader(manifestListFile.Stream);
        foreach (var manifestListEntry in manifestListReader.NextEntries)
        {
            var manifestFile = OpenFile(manifestListEntry.ManifestPath).GetAwaiter().GetResult();
            using var manifestReader = ManifestEntry.GetReader(manifestFile.Stream);
            foreach (var manifestEntry in manifestReader.NextEntries)
                yield return OpenFile(manifestEntry.DataFile.FilePath).GetAwaiter().GetResult();
        }
    }

    public IEnumerable<TRow> ReadRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)] TRow>(
        long? snapshotId = null) where TRow : IArrowSerializer<TRow>
    {
        var columnBuffers = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(16384)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        Read(snapshotId, columnBuffers).ContinueWith(_ => columnBuffers.Writer.TryComplete());

        foreach (var batch in columnBuffers.Reader.ReadAllAsync().ToBlockingEnumerable())
        {
            var rows = TRow.ListFromRecordBatch(batch);
            foreach (var row in rows) yield return row;
            batch.Dispose();
        }
    }

    public async Task Read(
        long? snapshotId,
        Channel<RecordBatch> results,
        CancellationToken cancellationToken = default)
    {
        if (!Table.IsLoaded) throw new InvalidOperationException("Cannot read uninitialized table");

        var snapshot = GetSnapshotOrLatest(snapshotId);

        var manifestListEntries = Channel.CreateBounded<ManifestListEntry>(
            new BoundedChannelOptions(8192)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true
            });

        var manifestEntries = Channel.CreateBounded<ManifestEntry>(
            new BoundedChannelOptions(8192)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        var snapshotRead = ReadSnapshotAsync(snapshot.SnapshotId, manifestListEntries, cancellationToken);

        var manifestReaders = Parallel.ForEachAsync(
            manifestListEntries.Reader.ReadAllAsync(cancellationToken),
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 4
            },
            async (entry, token) => { await ReadManifestAsync(entry, manifestEntries, token); });

        var dataFileReaders = Parallel.ForEachAsync(
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
        var schema = CSharpSchema.ToIcebergSchema(typeof(TRow), schemaId, _ => nextFieldId++);

        var channel = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(2048)
            {
                SingleWriter = true
            });

        var convertToArrow = Task.Run(
            async () =>
            {
                foreach (var chunk in rows.Chunk(16384))
                {
                    var batch = TRow.ToRecordBatch(chunk);
                    await channel.Writer.WriteAsync(batch, cancellationToken);
                }
            },
            cancellationToken);

        var append = Append(channel, schema, cancellationToken);

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
        var schema = CSharpSchema.ToIcebergSchema(typeof(TRow), schemaId, _ => nextFieldId++);

        var channel = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(2048)
            {
                SingleWriter = true
            });

        var convertToArrow = Task.Run(
            async () =>
            {
                foreach (var chunk in rows.Chunk(16384))
                {
                    var batch = RecordBatchBuilder.FromObjects(chunk);
                    await channel.Writer.WriteAsync(batch, cancellationToken);
                }
            },
            cancellationToken);

        var append = Append(channel, schema, cancellationToken);

        await convertToArrow;
        channel.Writer.TryComplete();
        await append;
    }

    internal async Task Append(
        Channel<RecordBatch> data,
        Schema schema,
        CancellationToken cancellationToken = default)
    {
        var partitionSpec = new PartitionSpec([], 0);
        var sortOrder = new SortOrder([], 0);

        await EnsureTableInitialized(schema, partitionSpec, sortOrder, cancellationToken);

        schema = GetSchema();

        var dataFiles = Channel.CreateBounded<DataFileWriteResult>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        var parallelOptions = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = 32
        };

        var dataFileWrite = Parallel.ForEachAsync(
            Enumerable.Range(0, 1),
            parallelOptions,
            async (i, token) =>
            {
                await WriteDataFileAsync(
                    data,
                    schema,
                    dataFiles,
                    token);
            });

        var snapshotId = Utils.GenerateSnapshotId();

        var existingManifests = Channel.CreateBounded<ManifestListEntry>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        var existingSnapshotRead = Task.CompletedTask;
        if (Table.Metadata!.CurrentSnapshotId > 0)
            existingSnapshotRead = ReadSnapshotAsync(
                Table.Metadata!.CurrentSnapshotId.Value,
                existingManifests,
                cancellationToken);

        var newManifests = Channel.CreateBounded<ManifestFileWriteResult>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        // TODO allow writing multiple manifests for scaling
        var manifestWrite = WriteManifestAsync(
            snapshotId,
            schema,
            partitionSpec,
            dataFiles,
            newManifests,
            cancellationToken);

        var snapshotCreate = CreateSnapshotAsync(
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

        var snapshot = await snapshotCreate;

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
        await using var dataFileStream = await OpenFile(dataFile.FilePath, cancellationToken);
        // using var faucet = CreateFaucet(dataFileStream.Stream, schema);

        using var arrowReaderProperties = ArrowReaderProperties.GetDefault();
        using var parquetReaderProperties = ReaderProperties.GetDefaultReaderProperties();
        using var arrowReader = new FileReader(
            dataFileStream.Stream,
            parquetReaderProperties,
            arrowReaderProperties,
            true);
        using var recordBatchReader = arrowReader.GetRecordBatchReader();

        while (!cancellationToken.IsCancellationRequested)
        {
            // TODO exceptions are swallowed
            var batch = await recordBatchReader.ReadNextRecordBatchAsync(cancellationToken);
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
        await using var dataFile = await NewDataFile(cancellationToken);
        using var arrowWriterProperties = ArrowWriterProperties.GetDefault();
        using var parquetWriterPropertiesBuilder = new WriterPropertiesBuilder();
        parquetWriterPropertiesBuilder.Compression(Compression.Zstd);
        parquetWriterPropertiesBuilder.MaxRowGroupLength(256 * 1024);
        var arrowSchema = ArrowSchema.FromSchema(schema);
        using var arrowWriter = new FileWriter(
            dataFile.Stream,
            arrowSchema,
            parquetWriterPropertiesBuilder.Build(),
            arrowWriterProperties,
            true);

        long written = 0;

        await foreach (var batch in batches.Reader.ReadAllAsync(cancellationToken))
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
        var snapshot = Table.Metadata!.SnapshotsById[snapshotId];

        var manifestListFile = await OpenFile(snapshot.ManifestList, cancellationToken);

        using var manifestListAppender = ManifestListEntry.GetReader(manifestListFile.Stream);

        foreach (var entry in manifestListAppender.NextEntries)
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
        var summary = new Summary { Operation = SummaryOperation.Overwrite };

        var manifestListFile = await CreateManifestListFile(snapshotId, sequenceNumber, cancellationToken);

        using var manifestListAppender = ManifestListEntry.GetAppender(
            manifestListFile.Stream,
            snapshotId,
            null,
            sequenceNumber);

        await foreach (var entry in newEntries.Reader.ReadAllAsync(cancellationToken))
        {
            summary.AddedDataFiles += entry.AddedFileCount;
            summary.AddedRecords += entry.AddedRowsCount;
            summary.AddedFilesSize += entry.AddedFilesSize;

            var manifestListEntry = new ManifestListEntry
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

        await foreach (var entry in existingEntries.Reader.ReadAllAsync(cancellationToken))
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
        var manifestFile = await OpenFile(manifestListEntry.ManifestPath, cancellationToken);

        using var manifestReader = ManifestEntry.GetReader(manifestFile.Stream);

        foreach (var entry in manifestReader.NextEntries)
        {
            // TODO only inherit if status = added
            var inheritedEntry = entry with
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
        var manifestFile = await CreateManifestFile(cancellationToken);

        using var manifestAppender = ManifestEntry.GetAppender(
            manifestFile.Stream,
            schema,
            partitionSpec,
            Content.Data);

        long addedFilesSize = 0;
        long addedRowsCount = 0;
        var addedDataFilesCount = 0;

        await foreach (var entry in dataFiles.Reader.ReadAllAsync(cancellationToken))
        {
            addedRowsCount += entry.RecordCount;
            addedDataFilesCount++;
            addedFilesSize += entry.FileSize;
            var manifestEntry = new ManifestEntry
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
        var parquetFilePath = new Uri(Table.DataFolderUri, Utils.GetParquetFileName(0, 0, Guid.NewGuid()));
        var parquetStream = await Table.ObjectStorage.Open(
            parquetFilePath,
            FileMode.CreateNew,
            cancellationToken);
        return new PathAndStream(parquetFilePath, parquetStream);
    }

    private async ValueTask<PathAndStream> CreateManifestFile(CancellationToken cancellationToken = default)
    {
        var manifestFilePath = new Uri(
            Table.MetadataFolderUri,
            ManifestEntry.GetFileName(Guid.NewGuid(), 0));
        var stream = await Table.ObjectStorage.Open(
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
        var manifestListFilePath = new Uri(
            Table.MetadataFolderUri,
            ManifestListEntry.GetFileName(snapshotId, sequenceNumber, Guid.NewGuid()));
        var manifestListStream = await Table.ObjectStorage.Open(
            manifestListFilePath,
            FileMode.CreateNew,
            cancellationToken);
        return new PathAndStream(manifestListFilePath, manifestListStream);
    }

    private async Task<PathAndStream> OpenFile(
        string path,
        CancellationToken cancellationToken = default)
    {
        var success = Uri.TryCreate(path, UriKind.RelativeOrAbsolute, out var uri);
        if (!success) throw new ArgumentException($"Invalid URI: {path}");

        var manifestListStream = await Table.ObjectStorage.Open(
            uri!,
            FileMode.Open,
            cancellationToken);
        return new PathAndStream(uri!, manifestListStream);
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
        var response = await Table.Catalog.UpdateTableAsync(
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
            if (Table.Metadata!.SnapshotsById.TryGetValue(snapshotId.Value, out var result))
                return result;
            else
                throw new ArgumentOutOfRangeException(nameof(snapshotId));
        }
        else
        {
            var currentSnapshotId = Table.Metadata!.CurrentSnapshotId ??
                                    throw new InvalidOperationException("Table doesn't have any snapshots");
            if (Table.Metadata.SnapshotsById.TryGetValue(currentSnapshotId, out var result))
                return result;
            else
                throw new UnreachableException("Could not find the current snapshot");
        }
    }

    private Schema GetSchema()
    {
        return Table.Metadata!.SchemasById[Table.Metadata!.CurrentSchemaId!.Value];
    }
}