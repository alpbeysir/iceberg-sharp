using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using EngineeredWood.IO;
using Iceberg.Net.Data;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Metadata;
using Iceberg.Net.Misc;
using Iceberg.Net.Query;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Schemas;
using Iceberg.Net.Storage;
using Microsoft.Extensions.Logging;
using Schema = Iceberg.Net.Schemas.Schema;
using SortOrder = Iceberg.Net.Metadata.SortOrder;

namespace Iceberg.Net.Catalog;

public sealed class TableOperations
{
    private readonly Identifier _identifier;
    private readonly ICatalog _catalog;
    private readonly ILogger<TableOperations> _logger;
    private Table? _table;

    internal TableOperations(Table table)
    {
        _identifier = table.Identifier;
        _catalog = table.Catalog;
        _logger = _catalog.LoggerFactory.CreateLogger<TableOperations>();
        _table = table;
    }

    internal TableOperations(Identifier identifier, ICatalog catalog)
    {
        _identifier = identifier;
        _catalog = catalog;
        _logger = _catalog.LoggerFactory.CreateLogger<TableOperations>();
    }

    private Table CurrentTable => _table ??
                                  throw new InvalidOperationException("The table has not been loaded or created.");

    public async Task
        FastAppendRowsAot<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)] TRow>(
            IEnumerable<TRow> rows,
            CancellationToken cancellationToken = default) where TRow : IArrowSerializer<TRow>
    {
        int schemaId = _table?.Metadata.CurrentSchemaId ?? 0;
        int nextFieldId = (_table?.Metadata.LastColumnId ?? 0) + 1;
        Schema schema = CSharpSchemas.ToIcebergSchema(typeof(TRow), schemaId, _ => nextFieldId++);

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
                    await PipelineMetrics.WriteAsync(
                        channel.Writer,
                        batch,
                        PipelineStage.ArrowConversion,
                        cancellationToken);
                }
            },
            cancellationToken);

        Task append = FastAppendArrow(channel, schema, cancellationToken);

        await convertToArrow;
        channel.Writer.TryComplete();
        await append;
    }

    [RequiresUnreferencedCode(
        "Uses reflection to inspect properties. Use AppendRowsAot for AOT-safe serialization.")]
    public async Task FastAppendRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)] TRow>(
        IEnumerable<TRow> rows,
        CancellationToken cancellationToken = default)
    {
        int schemaId = _table?.Metadata.CurrentSchemaId ?? 0;
        int nextFieldId = (_table?.Metadata.LastColumnId ?? 0) + 1;
        Schema schema = CSharpSchemas.ToIcebergSchema(typeof(TRow), schemaId, _ => nextFieldId++);

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
                    await PipelineMetrics.WriteAsync(
                        channel.Writer,
                        batch,
                        PipelineStage.ArrowConversion,
                        cancellationToken);
                }
            },
            cancellationToken);

        Task append = FastAppendArrow(channel, schema, cancellationToken);

        await convertToArrow;
        channel.Writer.TryComplete();
        await append;
    }

    private async Task FastAppendArrow(
        Channel<RecordBatch> data,
        Schema schema,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting append to table {TableIdentifier}", _identifier.ToString());
        PartitionSpec partitionSpec = new([], 0);
        SortOrder sortOrder = new([], 0);

        PendingChanges pendingChanges = await EnsureTableInitialized(
            schema,
            partitionSpec,
            sortOrder,
            cancellationToken);

        Schema tableSchema = GetSchema();
        if (!new IcebergTypeComparer().Equals(schema, tableSchema))
            throw new InvalidOperationException(
                "The appended row schema does not match the table schema.");

        schema = tableSchema;

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

        long snapshotId = Utils.GenerateSnapshotId();

        Channel<ManifestListEntry> existingManifests = Channel.CreateBounded<ManifestListEntry>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        Task existingSnapshotRead = Task.CompletedTask;
        Table currentTable = CurrentTable;
        if (currentTable.Metadata.CurrentSnapshotId > 0)
            existingSnapshotRead = ReadSnapshotAsync(
                currentTable,
                currentTable.Metadata.CurrentSnapshotId.Value,
                existingManifests.Writer,
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
            schema,
            partitionSpec,
            cancellationToken);

        await dataFileWrite;
        dataFiles.Writer.Complete();

        await manifestWrite;
        newManifests.Writer.Complete();

        await existingSnapshotRead;
        existingManifests.Writer.Complete();

        Snapshot snapshot = await snapshotCreate;
        _logger.LogInformation(
            "Created snapshot {SnapshotId} for table {TableIdentifier} with {AddedRecords} records in {AddedDataFiles} data files",
            snapshot.SnapshotId,
            _identifier.ToString(),
            snapshot.Summary.AddedRecords,
            snapshot.Summary.AddedDataFiles);

        pendingChanges.Updates.AddRange(
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
        await CommitChanges(pendingChanges, cancellationToken);
        _logger.LogInformation(
            "Completed append to table {TableIdentifier} at snapshot {SnapshotId}",
            _identifier.ToString(),
            snapshot.SnapshotId);
    }

    private async Task WriteDataFileAsync(
        Channel<RecordBatch> batches,
        Schema schema,
        Channel<DataFileWriteResult> results,
        CancellationToken cancellationToken)
    {
        string configuredFormat = CurrentTable.Properties.GetString(
            TableProperties.DefaultFileFormat,
            TableProperties.DefaultFileFormatDefault);
        IDataFileFormat dataFileFormat = DataFileFormatRegistry.Resolve(
            configuredFormat,
            CurrentTable.Properties);
        await using PathAndFile<ISequentialFile> dataFile = await CreateDataFile(
            dataFileFormat.FileExtension,
            cancellationToken);
        await using SequentialFileStream dataFileStream = new(dataFile.File);
        long written = await dataFileFormat.WriteAsync(
            dataFileStream,
            schema,
            batches.Reader,
            cancellationToken);

        _logger.LogDebug(
            "Wrote data file {DataFilePath} with {RecordCount} records and {FileSize} bytes",
            dataFile.Path,
            written,
            dataFile.File.Position);

        await PipelineMetrics.WriteAsync(
            results.Writer,
            new DataFileWriteResult(
                dataFile.Path,
                dataFileFormat.Format,
                written,
                dataFile.File.Position),
            PipelineStage.DataFileWrite,
            cancellationToken);
    }

    private async Task<Snapshot> CreateSnapshotAsync(
        long snapshotId,
        long? parentSnapshotId,
        Channel<ManifestListEntry> existingEntries,
        Channel<ManifestFileWriteResult> newEntries,
        Schema schema,
        PartitionSpec currentPartitionSpec,
        CancellationToken cancellationToken = default)
    {
        long sequenceNumber = (long)CurrentTable.Metadata.LastSequenceNumber! + 1;
        Summary summary = new()
        {
            Operation = SummaryOperation.Append,
            AddedDataFiles = 0,
            AddedRecords = 0,
            AddedFilesSize = 0
        };

        PathAndFile<ISequentialFile> manifestListFile = await CreateManifestListFile(
            snapshotId,
            sequenceNumber,
            cancellationToken);
        IReadOnlyList<PartitionSpec> partitionSpecs = CurrentTable.Metadata
            .PartitionSpecs
            .Append(currentPartitionSpec)
            .DistinctBy(spec => spec.SpecId)
            .ToArray();

        await using (SequentialFileStream manifestListStream = new(manifestListFile.File))
        using (ManifestWriter<ManifestListEntry> manifestListAppender = ManifestIO.CreateManifestListWriter(
                   manifestListStream,
                   snapshotId,
                   null,
                   sequenceNumber,
                   schema,
                   partitionSpecs))
        {
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
            }

            await foreach (ManifestListEntry entry in existingEntries.Reader.ReadAllAsync(cancellationToken))
                manifestListAppender.Append(entry);
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
            SchemaId = schema.SchemaId!.Value
        };
    }

    private async Task WriteManifestAsync(
        long snapshotId,
        Schema schema,
        PartitionSpec partitionSpec,
        Channel<DataFileWriteResult> dataFiles,
        Channel<ManifestFileWriteResult> results,
        CancellationToken cancellationToken)
    {
        PathAndFile<ISequentialFile> manifestFile = await CreateManifestFile(cancellationToken);

        long addedFilesSize = 0;
        long addedRowsCount = 0;
        int addedDataFilesCount = 0;

        await using (SequentialFileStream manifestStream = new(manifestFile.File))
        using (ManifestWriter<ManifestEntry> manifestAppender = ManifestIO.CreateManifestWriter(
                   manifestStream,
                   schema,
                   partitionSpec,
                   Content.Data))
        {
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
                        FileFormat = entry.Format,
                        Partition = [],
                        RecordCount = entry.RecordCount,
                        FileSizeInBytes = entry.FileSize
                    }
                };
                manifestAppender.Append(manifestEntry);
            }
        }

        await PipelineMetrics.WriteAsync(
            results.Writer,
            new ManifestFileWriteResult(
                manifestFile.Path,
                manifestFile.File.Position,
                addedRowsCount,
                addedDataFilesCount,
                addedFilesSize),
            PipelineStage.ManifestWrite,
            cancellationToken);

        _logger.LogDebug(
            "Wrote manifest {ManifestPath} with {AddedDataFiles} data files and {AddedRecords} records",
            manifestFile.Path,
            addedDataFilesCount,
            addedRowsCount);

        await manifestFile.DisposeAsync();
    }

    private async ValueTask<PathAndFile<ISequentialFile>> CreateDataFile(
        string fileExtension,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(fileExtension) ||
            !fileExtension.StartsWith('.') ||
            fileExtension.Contains('/') ||
            fileExtension.Contains('\\'))
            throw new InvalidOperationException(
                $"Data file format returned invalid file extension '{fileExtension}'");

        Uri dataFilePath = new(
            CurrentTable.DataFolderUri,
            $"00000-0-{Guid.NewGuid()}{fileExtension}");
        ISequentialFile dataFile = await CurrentTable.CreateFile(
            dataFilePath,
            overwrite: true,
            cancellationToken: cancellationToken);
        return new PathAndFile<ISequentialFile>(dataFilePath, dataFile);
    }

    private async ValueTask<PathAndFile<ISequentialFile>> CreateManifestFile(
        CancellationToken cancellationToken = default)
    {
        Uri manifestFilePath = new(
            CurrentTable.MetadataFolderUri,
            ManifestEntry.GetFileName(Guid.NewGuid(), 0));
        ISequentialFile file = await CurrentTable.CreateFile(
            manifestFilePath,
            overwrite: true,
            cancellationToken: cancellationToken);
        return new PathAndFile<ISequentialFile>(manifestFilePath, file);
    }

    private async ValueTask<PathAndFile<ISequentialFile>> CreateManifestListFile(
        long snapshotId,
        long sequenceNumber,
        CancellationToken cancellationToken = default)
    {
        Uri manifestListFilePath = new(
            CurrentTable.MetadataFolderUri,
            ManifestListEntry.GetFileName(snapshotId, sequenceNumber, Guid.NewGuid()));
        ISequentialFile manifestListFile = await CurrentTable.CreateFile(
            manifestListFilePath,
            overwrite: true,
            cancellationToken: cancellationToken);
        return new PathAndFile<ISequentialFile>(manifestListFilePath, manifestListFile);
    }

    private async Task<PendingChanges> EnsureTableInitialized(
        Schema schema,
        PartitionSpec partitionSpec,
        SortOrder sortOrder,
        CancellationToken cancellationToken = default)
    {
        PendingChanges pendingChanges = new([], []);
        if (_table is not null) return pendingChanges;

        _logger.LogDebug("Loading table {TableIdentifier} before append", _identifier.ToString());
        _table = await _catalog.LoadTableAsync(
            _identifier,
            cancellationToken: cancellationToken);
        if (_table is null)
        {
            _logger.LogInformation("Table {TableIdentifier} does not exist; creating it", _identifier.ToString());
            _table = await _catalog.CreateTableAsync(
                _identifier,
                schema,
                true,
                cancellationToken);
            pendingChanges.Requirements.Add(new AssertCreate());
            pendingChanges.Updates.AddRange(
            [
                new SetLocationTableUpdate(CurrentTable.Metadata.Location),
                new AddSchemaTableUpdate(CurrentTable.Metadata.Schemas[0]),
                new AddPartitionSpecTableUpdate(partitionSpec),
                new AddSortOrderTableUpdate(sortOrder)
            ]);
        }
        else
        {
            _logger.LogDebug(
                "Loaded table {TableIdentifier} at snapshot {SnapshotId}",
                _identifier.ToString(),
                _table.Metadata.CurrentSnapshotId);
        }

        return pendingChanges;
    }

    private async Task CommitChanges(PendingChanges pendingChanges, CancellationToken cancellationToken)
    {
        // TODO retry (could also be handled in catalog)
        _logger.LogDebug(
            "Committing append to table {TableIdentifier} with {UpdateCount} updates and {RequirementCount} requirements",
            _identifier.ToString(),
            pendingChanges.Updates.Count,
            pendingChanges.Requirements.Count);
        _table = await _catalog.UpdateTableAsync(
            CurrentTable,
            pendingChanges.Updates,
            pendingChanges.Requirements,
            cancellationToken);
    }

    public IEnumerable<TRow> ReadRows<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
        TRow>(long? snapshotId = null) where TRow : IArrowSerializer<TRow>
    {
        Table table = CurrentTable;
        Snapshot snapshot = GetSnapshotOrLatest(table, snapshotId);
        Schema snapshotSchema = GetSnapshotSchema(table, snapshot);
        VerifyRowSchema<TRow>(snapshot, snapshotSchema);

        Channel<RecordBatch> columnBuffers = Channel.CreateBounded<RecordBatch>(
            new BoundedChannelOptions(512)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        Task read = ReadArrow(table, snapshot.SnapshotId, columnBuffers.Writer);
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
            _identifier.ToString(),
            rowCount);
    }

    private async Task ReadArrow(
        Table table,
        long snapshotId,
        ChannelWriter<RecordBatch> results,
        CancellationToken cancellationToken = default)
    {
        Channel<ManifestEntry> manifestEntries = Channel.CreateBounded<ManifestEntry>(
            new BoundedChannelOptions(8192)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        Task manifestRead = ReadManifestEntries(
            table,
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
            async (entry, token) => { await ReadDataFileAsync(table, entry.DataFile, results, token); });

        await manifestRead;
        manifestEntries.Writer.Complete();

        await dataFileReaders;
    }

    public async Task ReadManifestEntries(
        ChannelWriter<ManifestEntry> results,
        long? snapshotId = null,
        CancellationToken cancellationToken = default)
    {
        Table table = CurrentTable;
        await ReadManifestEntries(table, results, snapshotId, cancellationToken);
    }

    private async Task ReadManifestEntries(
        Table table,
        ChannelWriter<ManifestEntry> results,
        long? snapshotId,
        CancellationToken cancellationToken)
    {
        Snapshot snapshot = GetSnapshotOrLatest(table, snapshotId);
        _logger.LogInformation(
            "Scanning table {TableIdentifier} at snapshot {SnapshotId}",
            _identifier.ToString(),
            snapshot.SnapshotId);

        Channel<ManifestListEntry> manifestListEntries = Channel.CreateBounded<ManifestListEntry>(
            new BoundedChannelOptions(8192)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true
            });

        Task snapshotRead = ReadSnapshotAsync(
            table,
            snapshot.SnapshotId,
            manifestListEntries.Writer,
            cancellationToken);

        Task manifestReaders = Parallel.ForEachAsync(
            manifestListEntries.Reader.ReadAllAsync(cancellationToken),
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = 16
            },
            async (entry, token) => { await ReadManifestAsync(table, entry, results, token); });

        await snapshotRead;
        manifestListEntries.Writer.Complete();
        await manifestReaders;
        _logger.LogDebug(
            "Completed manifest scan for table {TableIdentifier} at snapshot {SnapshotId}",
            _identifier.ToString(),
            snapshot.SnapshotId);
    }

    private async Task ReadSnapshotAsync(
        Table table,
        long snapshotId,
        ChannelWriter<ManifestListEntry> results,
        CancellationToken cancellationToken = default)
    {
        Snapshot snapshot = table.Metadata.SnapshotsById[snapshotId];
        _logger.LogDebug(
            "Reading manifest list {ManifestListPath} for snapshot {SnapshotId}",
            snapshot.ManifestList,
            snapshotId);
        Schema schema = GetSnapshotSchema(table, snapshot);

        await using PathAndFile<IRandomAccessFile> manifestListFile = await OpenFile(
            table,
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
        Table table,
        ManifestListEntry manifestListEntry,
        ChannelWriter<ManifestEntry> results,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Reading manifest {ManifestPath} for table {TableIdentifier}",
            manifestListEntry.ManifestPath,
            _identifier.ToString());
        await using PathAndFile<IRandomAccessFile> manifestFile = await OpenFile(
            table,
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
        Table table,
        DataFile dataFile,
        ChannelWriter<RecordBatch> results,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Reading data file {DataFilePath} for table {TableIdentifier}",
            dataFile.FilePath,
            _identifier.ToString());
        await using PathAndFile<IRandomAccessFile> storageFile = await OpenFile(
            table,
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

    private static async Task<PathAndFile<IRandomAccessFile>> OpenFile(
        Table table,
        string path,
        CancellationToken cancellationToken = default)
    {
        if (!Uri.TryCreate(path, UriKind.RelativeOrAbsolute, out Uri? uri))
            throw new ArgumentException($"Invalid URI: {path}");

        IRandomAccessFile file = await table.ReadFile(uri, cancellationToken);
        return new PathAndFile<IRandomAccessFile>(uri, file);
    }

    private static Snapshot GetSnapshotOrLatest(Table table, long? snapshotId)
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

    private static Schema GetSnapshotSchema(Table table, Snapshot snapshot)
    {
        int schemaId = snapshot.SchemaId ?? table.Metadata.CurrentSchemaId ??
            throw new InvalidDataException("Table metadata does not identify a schema for the snapshot.");
        return table.Metadata.SchemasById.TryGetValue(schemaId, out Schema? schema)
            ? schema
            : throw new InvalidDataException(
                $"Snapshot {snapshot.SnapshotId} refers to unknown schema ID {schemaId}.");
    }

    private static void VerifyRowSchema<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
        TRow>(Snapshot snapshot, Schema snapshotSchema)
    {
        IReadOnlyDictionary<string, int> fieldIds = SchemaUtilities.FieldIdsByPath(snapshotSchema);
        Schema requestedSchema = CSharpSchemas.ToIcebergSchema(
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

    private sealed record PendingChanges(
        List<ITableRequirement> Requirements,
        List<ITableUpdate> Updates);

    private Schema GetSchema()
    {
        return CurrentTable.Metadata.SchemasById[CurrentTable.Metadata.CurrentSchemaId!.Value];
    }
}
