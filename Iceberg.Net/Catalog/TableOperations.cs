using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Misc;
using Iceberg.Net.Query;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Schemas;
using Iceberg.Net.Storage;
using Schema = Iceberg.Net.Schemas.Schema;
using SortOrder = Iceberg.Net.Metadata.SortOrder;

namespace Iceberg.Net.Catalog;

public sealed class TableOperations(Table table)
{
    private Table Table { get; set; } = table;

    public async Task FastAppendRowsAot<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)] TRow>(
        IEnumerable<TRow> rows,
        CancellationToken cancellationToken = default) where TRow : IArrowSerializer<TRow>
    {
        int schemaId = Table.Metadata?.CurrentSchemaId ?? 0;
        int nextFieldId = (Table.Metadata?.LastColumnId ?? 0) + 1;
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
        int schemaId = Table.Metadata?.CurrentSchemaId ?? 0;
        int nextFieldId = (Table.Metadata?.LastColumnId ?? 0) + 1;
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
        PartitionSpec partitionSpec = new([], 0);
        SortOrder sortOrder = new([], 0);

        PendingChanges pendingChanges = await EnsureTableInitialized(
            schema,
            partitionSpec,
            sortOrder,
            cancellationToken);

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

        long snapshotId = Utils.GenerateSnapshotId();

        Channel<ManifestListEntry> existingManifests = Channel.CreateBounded<ManifestListEntry>(
            new BoundedChannelOptions(1024)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });

        Task existingSnapshotRead = Task.CompletedTask;
        if (Table.Metadata!.CurrentSnapshotId > 0)
            existingSnapshotRead = new TableScan(Table).ReadSnapshotAsync(
                Table.Metadata!.CurrentSnapshotId.Value,
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
            schema.SchemaId!.Value,
            cancellationToken);

        await dataFileWrite;
        dataFiles.Writer.Complete();

        await manifestWrite;
        newManifests.Writer.Complete();

        await existingSnapshotRead;
        existingManifests.Writer.Complete();

        Snapshot snapshot = await snapshotCreate;

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
    }

    private async Task WriteDataFileAsync(
        Channel<RecordBatch> batches,
        Schema schema,
        Channel<DataFileWriteResult> results,
        CancellationToken cancellationToken)
    {
        string configuredFormat = Table.Properties.GetString(
            TableProperties.DefaultFileFormat,
            TableProperties.DefaultFileFormatDefault);
        IDataFileFormat dataFileFormat = DataFileFormatRegistry.Resolve(
            configuredFormat,
            Table.Properties);
        await using PathAndStream dataFile = await CreateDataFile(
            dataFileFormat.FileExtension,
            cancellationToken);
        long written = await dataFileFormat.WriteAsync(
            dataFile.Stream,
            schema,
            batches.Reader,
            cancellationToken);

        await results.Writer.WriteAsync(
            new DataFileWriteResult(
                dataFile.Path,
                dataFileFormat.Format,
                written,
                dataFile.Stream.Length),
            cancellationToken);
    }

    private async Task<Snapshot> CreateSnapshotAsync(
        long snapshotId,
        long? parentSnapshotId,
        Channel<ManifestListEntry> existingEntries,
        Channel<ManifestFileWriteResult> newEntries,
        int schemaId,
        CancellationToken cancellationToken = default)
    {
        long sequenceNumber = (long)Table.Metadata!.LastSequenceNumber! + 1;
        Summary summary = new() { Operation = SummaryOperation.Append };

        PathAndStream manifestListFile = await CreateManifestListFile(snapshotId, sequenceNumber, cancellationToken);

        using (ManifestWriter<ManifestListEntry> manifestListAppender = ManifestIO.CreateManifestListWriter(
                   manifestListFile.Stream,
                   snapshotId,
                   null,
                   sequenceNumber))
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
            SchemaId = schemaId
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
        PathAndStream manifestFile = await CreateManifestFile(cancellationToken);

        long addedFilesSize = 0;
        long addedRowsCount = 0;
        int addedDataFilesCount = 0;

        using (ManifestWriter<ManifestEntry> manifestAppender = ManifestIO.CreateManifestWriter(
                   manifestFile.Stream,
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

    private async ValueTask<PathAndStream> CreateDataFile(
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
            Table.DataFolderUri,
            $"00000-0-{Guid.NewGuid()}{fileExtension}");
        Stream dataFileStream = await Table.Open(
            dataFilePath,
            FileMode.CreateNew,
            cancellationToken);
        return new PathAndStream(dataFilePath, dataFileStream);
    }

    private async ValueTask<PathAndStream> CreateManifestFile(CancellationToken cancellationToken = default)
    {
        Uri manifestFilePath = new(
            Table.MetadataFolderUri,
            ManifestEntry.GetFileName(Guid.NewGuid(), 0));
        Stream stream = await Table.Open(
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
        Stream manifestListStream = await Table.Open(
            manifestListFilePath,
            FileMode.CreateNew,
            cancellationToken);
        return new PathAndStream(manifestListFilePath, manifestListStream);
    }

    private async Task<PendingChanges> EnsureTableInitialized(
        Schema schema,
        PartitionSpec partitionSpec,
        SortOrder sortOrder,
        CancellationToken cancellationToken = default)
    {
        PendingChanges pendingChanges = new([], []);
        if (Table.IsLoaded) return pendingChanges;
        try
        {
            Table =
                await Table.Catalog.LoadTableAsync(Table.Identifier, cancellationToken: cancellationToken);
        }
        catch (Exception)
        {
            Table = await Table.Catalog.CreateTableAsync(
                Table.Identifier,
                schema,
                true,
                cancellationToken);
            pendingChanges.Requirements.Add(new AssertCreate());
            pendingChanges.Updates.AddRange(
            [
                new SetLocationTableUpdate(Table.Metadata!.Location),
                new AddSchemaTableUpdate(Table.Metadata!.Schemas[0]),
                new AddPartitionSpecTableUpdate(partitionSpec),
                new AddSortOrderTableUpdate(sortOrder)
            ]);
        }

        return pendingChanges;
    }

    private async Task CommitChanges(PendingChanges pendingChanges, CancellationToken cancellationToken)
    {
        // TODO retry (could also be handled in catalog)
        Table = await Table.Catalog.UpdateTableAsync(
            Table,
            pendingChanges.Updates,
            pendingChanges.Requirements,
            cancellationToken);
    }

    private sealed record PendingChanges(
        List<ITableRequirement> Requirements,
        List<ITableUpdate> Updates);

    private Schema GetSchema()
    {
        return Table.Metadata!.SchemasById[Table.Metadata!.CurrentSchemaId!.Value];
    }
}
