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

    public TableOperations(Table table)
    {
        _identifier = table.Identifier;
        _catalog = table.Catalog;
        _logger = _catalog.LoggerFactory.CreateLogger<TableOperations>();
        _table = table;
    }

    public TableOperations(Identifier identifier, ICatalog catalog)
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
        if (CurrentTable.Metadata.CurrentSnapshotId > 0)
            existingSnapshotRead = new TableScan(CurrentTable).ReadSnapshotAsync(
                CurrentTable.Metadata.CurrentSnapshotId.Value,
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

    private sealed record PendingChanges(
        List<ITableRequirement> Requirements,
        List<ITableUpdate> Updates);

    private Schema GetSchema()
    {
        return CurrentTable.Metadata.SchemasById[CurrentTable.Metadata.CurrentSchemaId!.Value];
    }
}
