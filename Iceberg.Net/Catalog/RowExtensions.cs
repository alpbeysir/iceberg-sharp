using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Metadata;
using Iceberg.Net.Query;
using Iceberg.Net.Schemas;
using Microsoft.Extensions.Logging;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Catalog;

public static class RowExtensions
{
    extension(TableOperations operations)
    {
        public async Task FastAppendRowsAot<
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
            TRow>(IEnumerable<TRow> rows,
            CancellationToken cancellationToken = default) where TRow : IArrowSerializer<TRow>
        {
            (Channel<RecordBatch> data, Task convertToArrow) = ConvertToArrowAot(rows, cancellationToken);

            Schema schema = CreateSchema<TRow>(operations);
            Task append = operations.ApplyAsync(
                new AppendFilesOperation
                {
                    Data = data,
                    Schema = schema
                },
                cancellationToken);

            await convertToArrow;
            data.Writer.TryComplete();
            await append;
        }

        [RequiresUnreferencedCode(
            "Uses reflection to inspect properties. Use FastAppendRowsAot for AOT-safe serialization.")]
        public async Task FastAppendRows<
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
            TRow>(IEnumerable<TRow> rows,
            CancellationToken cancellationToken = default)
        {
            Channel<RecordBatch> data = Channel.CreateBounded<RecordBatch>(
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
                            data.Writer,
                            batch,
                            PipelineStage.ArrowConversion,
                            cancellationToken);
                    }
                },
                cancellationToken);

            Schema schema = CreateSchema<TRow>(operations);
            Task append = operations.ApplyAsync(
                new AppendFilesOperation
                {
                    Data = data,
                    Schema = schema
                },
                cancellationToken);

            await convertToArrow;
            data.Writer.TryComplete();
            await append;
        }

        public IEnumerable<TRow> ReadRows<
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
            TRow>(long? snapshotId = null) where TRow : IArrowSerializer<TRow>
        {
            (Snapshot snapshot, Schema snapshotSchema) = operations.ResolveSnapshot(snapshotId);
            VerifyRowSchema<TRow>(snapshot, snapshotSchema);

            Channel<RecordBatch> data = Channel.CreateBounded<RecordBatch>(
                new BoundedChannelOptions(512)
                {
                    FullMode = BoundedChannelFullMode.Wait
                });

            Task read = operations.ReadArrowAsync(data.Writer, snapshot.SnapshotId);
            _ = read.ContinueWith(
                completed => data.Writer.TryComplete(completed.Exception?.GetBaseException()),
                TaskScheduler.Default);

            long rowCount = 0;
            foreach (RecordBatch batch in data.Reader.ReadAllAsync().ToBlockingEnumerable())
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

            ILogger logger = operations.Catalog.LoggerFactory.CreateLogger(typeof(RowExtensions).FullName!);
            logger.LogInformation(
                "Completed scan of table {TableIdentifier} with {RowCount} rows",
                operations.Identifier.ToString(),
                rowCount);
        }
    }

    private static (Channel<RecordBatch> data, Task convertToArrow) ConvertToArrowAot<TRow>(IEnumerable<TRow> rows,
        CancellationToken cancellationToken) where TRow : IArrowSerializer<TRow>
    {
        Channel<RecordBatch> data = Channel.CreateBounded<RecordBatch>(
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
                        data.Writer,
                        batch,
                        PipelineStage.ArrowConversion,
                        cancellationToken);
                }
            },
            cancellationToken);
        return (data, convertToArrow);
    }

    private static Schema CreateSchema<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
        TRow>(
        TableOperations operations)
    {
        int schemaId = operations.Table?.Metadata.CurrentSchemaId ?? 0;
        int nextFieldId = (operations.Table?.Metadata.LastColumnId ?? 0) + 1;
        return CSharpSchemas.ToIcebergSchema(typeof(TRow), schemaId, _ => nextFieldId++);
    }

    private static void VerifyRowSchema<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.AllProperties)]
        TRow>(
        Snapshot snapshot,
        Schema snapshotSchema)
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
}
