using System.Diagnostics;
using System.Threading.Channels;
using Apache.Arrow;
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

    internal Table? Table { get; private set; }

    internal TableOperations(Table table)
    {
        _identifier = table.Identifier;
        _catalog = table.Catalog;
        _logger = _catalog.LoggerFactory.CreateLogger<TableOperations>();
        Table = table;
    }

    internal TableOperations(Identifier identifier, ICatalog catalog)
    {
        _identifier = identifier;
        _catalog = catalog;
        _logger = _catalog.LoggerFactory.CreateLogger<TableOperations>();
    }

    internal ICatalog Catalog => _catalog;

    internal Identifier Identifier => _identifier;

    private Table GetTable()
    {
        return Table ?? throw new InvalidOperationException("The table has not been loaded or created.");
    }

    public Task ApplyAsync(
        ITableOperation operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        return operation switch
        {
            AppendOperation append => AppendFilesAsync(append, cancellationToken),
            _ => throw new NotSupportedException(
                $"Table operation '{operation.GetType().FullName}' is not supported.")
        };
    }

    private async Task AppendFilesAsync(
        AppendOperation operation,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting append to table {TableIdentifier}", _identifier.ToString());
        PartitionSpec partitionSpec = operation.PartitionSpec ??
                                      (Table is null ? PartitionSpec.Default : GetPartitionSpec());
        SortOrder sortOrder = operation.SortOrder ??
                              (Table is null ? SortOrder.Default : GetSortOrder());

        PendingChanges pendingChanges = await EnsureTableInitialized(
            operation.Schema,
            partitionSpec,
            sortOrder,
            cancellationToken);

        Schema tableSchema = GetSchema();
        if (!IcebergTypeComparer.Default.Equals(operation.Schema, tableSchema))
            throw new InvalidOperationException(
                "The appended row schema does not match the table schema.");

        Channel<DataFileWriteResult> dataFiles = Channel.CreateBounded<DataFileWriteResult>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        Task dataFileWrite = WriteDataFileAsync(
            operation.Data,
            tableSchema,
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
        Table currentTable = GetTable();
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
            tableSchema,
            partitionSpec,
            dataFiles,
            newManifests,
            cancellationToken);

        Task<Snapshot> snapshotCreate = CreateSnapshotAsync(
            snapshotId,
            null,
            existingManifests,
            newManifests,
            tableSchema,
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
            new SetCurrentSchemaTableUpdate((int)tableSchema.SchemaId!),
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
        Table table = GetTable();
        string configuredFormat = table.Properties.GetString(
            TableProperties.DefaultFileFormat,
            TableProperties.DefaultFileFormatDefault);
        IDataFileFormat dataFileFormat = DataFileFormatRegistry.Resolve(
            configuredFormat,
            table.Properties);
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
        Table table = GetTable();
        long sequenceNumber = (long)table.Metadata.LastSequenceNumber! + 1;
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
        IReadOnlyList<PartitionSpec> partitionSpecs = table.Metadata
            .PartitionSpecs
            .Append(currentPartitionSpec)
            .DistinctBy(spec => spec.SpecId)
            .ToArray();

        await using (SequentialFileStream manifestListStream = new(manifestListFile.File))
        await using (ManifestWriter<ManifestListEntry> manifestListAppender =
                     await ManifestIO.CreateManifestListWriterAsync(
                         manifestListStream,
                         snapshotId,
                         parentSnapshotId,
                         sequenceNumber,
                         schema,
                         partitionSpecs,
                         table.Properties,
                         cancellationToken))
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
                    PartitionSpecId = currentPartitionSpec.SpecId!.Value,
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
                await manifestListAppender.AppendAsync(manifestListEntry, cancellationToken);
            }

            await foreach (ManifestListEntry entry in existingEntries.Reader.ReadAllAsync(cancellationToken))
                await manifestListAppender.AppendAsync(entry, cancellationToken);
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
        await using (ManifestWriter<ManifestEntry> manifestAppender =
                     await ManifestIO.CreateManifestWriterAsync(
                         manifestStream,
                         schema,
                         partitionSpec,
                         Content.Data,
                         GetTable().Properties,
                         cancellationToken))
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
                await manifestAppender.AppendAsync(manifestEntry, cancellationToken);
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
        Table table = GetTable();
        if (string.IsNullOrWhiteSpace(fileExtension) ||
            !fileExtension.StartsWith('.') ||
            fileExtension.Contains('/') ||
            fileExtension.Contains('\\'))
            throw new InvalidOperationException(
                $"Data file format returned invalid file extension '{fileExtension}'");

        Uri dataFilePath = new(
            table.DataFolderUri,
            $"00000-0-{Guid.NewGuid()}{fileExtension}");
        ISequentialFile dataFile = await table.CreateFile(
            dataFilePath,
            overwrite: true,
            cancellationToken: cancellationToken);
        return new PathAndFile<ISequentialFile>(dataFilePath, dataFile);
    }

    private async ValueTask<PathAndFile<ISequentialFile>> CreateManifestFile(
        CancellationToken cancellationToken = default)
    {
        Table table = GetTable();
        Uri manifestFilePath = new(
            table.MetadataFolderUri,
            ManifestEntry.GetFileName(Guid.NewGuid(), 0));
        ISequentialFile file = await table.CreateFile(
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
        Table table = GetTable();
        Uri manifestListFilePath = new(
            table.MetadataFolderUri,
            ManifestListEntry.GetFileName(snapshotId, sequenceNumber, Guid.NewGuid()));
        ISequentialFile manifestListFile = await table.CreateFile(
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
        if (Table is not null) return pendingChanges;

        _logger.LogInformation("Table {TableIdentifier} does not exist; creating it", _identifier.ToString());
        Table createdTable = await _catalog.CreateTableAsync(
            _identifier,
            schema,
            true,
            cancellationToken);
        Table = createdTable;
        pendingChanges.Requirements.Add(new AssertCreate());
        pendingChanges.Updates.AddRange(
        [
            new SetLocationTableUpdate(createdTable.Metadata.Location),
            new AddSchemaTableUpdate(createdTable.Metadata.Schemas[0]),
            new AddPartitionSpecTableUpdate(partitionSpec),
            new AddSortOrderTableUpdate(sortOrder)
        ]);

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
        Table table = GetTable();
        Table = await _catalog.UpdateTableAsync(
            table,
            pendingChanges.Updates,
            pendingChanges.Requirements,
            cancellationToken);
    }

    public Task ReadArrowAsync(
        ChannelWriter<RecordBatch> results,
        long? snapshotId = null,
        CancellationToken cancellationToken = default)
    {
        Table table = GetTable();
        Snapshot snapshot = GetSnapshotOrLatest(table, snapshotId);
        return ReadArrowAsync(table, snapshot.SnapshotId, results, cancellationToken);
    }

    private async Task ReadArrowAsync(
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
        Table table = GetTable();
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
            MaybeInheritSequenceNumber,
            cancellationToken);
        return;

        ManifestEntry MaybeInheritSequenceNumber(ManifestEntry entry)
        {
            if (entry.Status != Status.Added && (entry.SequenceNumber == null || entry.FileSequenceNumber == null ||
                                                 entry.SnapshotId == null))
            {
                throw new InvalidOperationException(
                    "A manifest with status EXISTING or DELETED that doesn't have a sequence number was found");
            }

            return entry with
            {
                FileSequenceNumber = entry.FileSequenceNumber ?? manifestListEntry.SequenceNumber,
                SequenceNumber = entry.SequenceNumber ?? manifestListEntry.SequenceNumber,
                SnapshotId = entry.SnapshotId ?? manifestListEntry.AddedSnapshotId
            };
        }
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

    internal (Snapshot Snapshot, Schema Schema) ResolveSnapshot(long? snapshotId)
    {
        Table table = GetTable();
        Snapshot snapshot = GetSnapshotOrLatest(table, snapshotId);
        return (snapshot, GetSnapshotSchema(table, snapshot));
    }

    private sealed record PendingChanges(
        List<ITableRequirement> Requirements,
        List<ITableUpdate> Updates);

    private Schema GetSchema()
    {
        Table table = GetTable();
        return table.Metadata.SchemasById[table.Metadata.CurrentSchemaId!.Value];
    }

    private PartitionSpec GetPartitionSpec()
    {
        Table table = GetTable();
        int defaultSpecId = table.Metadata.DefaultSpecId ??
                            throw new InvalidDataException(
                                $"Table '{_identifier}' does not identify a default partition spec.");
        return table.Metadata.PartitionSpecs.SingleOrDefault(spec => spec.SpecId == defaultSpecId) ??
               throw new InvalidDataException(
                   $"Table '{_identifier}' refers to unknown default partition spec ID {defaultSpecId}.");
    }

    private SortOrder GetSortOrder()
    {
        Table table = GetTable();
        int defaultSortOrderId = table.Metadata.DefaultSortOrderId ??
                                 throw new InvalidDataException(
                                     $"Table '{_identifier}' does not identify a default sort order.");
        return table.Metadata.SortOrders.SingleOrDefault(order => order.OrderId == defaultSortOrderId) ??
               throw new InvalidDataException(
                   $"Table '{_identifier}' refers to unknown default sort order ID {defaultSortOrderId}.");
    }
}