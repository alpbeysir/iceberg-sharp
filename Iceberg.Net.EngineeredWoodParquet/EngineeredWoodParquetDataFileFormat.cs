using System.Threading.Channels;
using Apache.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Schema;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Schemas;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.EngineeredWoodParquet;

public sealed class EngineeredWoodParquetDataFileFormat(TablePropertyResolver properties)
    : IDataFileFormat
{
    public static IReadOnlySet<string> Formats { get; } =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "parquet" };

    public static IDataFileFormat Create(TablePropertyResolver properties) =>
        new EngineeredWoodParquetDataFileFormat(properties);

    public string Format => "parquet";

    public string FileExtension => ".parquet";

    public async Task ReadAsync(
        IRandomAccessFile file,
        Schema schema,
        ChannelWriter<RecordBatch> results,
        IReadOnlySet<int>? fieldIds = null,
        CancellationToken cancellationToken = default)
    {
        bool hasNestedFields = schema.Fields.Any(field => field.FieldType is not PrimitiveType);
        ParquetReadOptions options = EngineeredWoodParquetTableProperties.CreateReadOptions(
            properties,
            hasNestedFields);
        await using ParquetFileReader reader = new(file, ownsFile: false, options);
        IReadOnlyList<string>? columns = await ResolveColumnsAsync(reader, fieldIds, cancellationToken);

        await foreach (RecordBatch batch in reader.ReadAllAsync(columns, cancellationToken))
            await PipelineMetrics.WriteAsync(
                results,
                batch,
                PipelineStage.DataFileRead,
                cancellationToken);
    }

    public async ValueTask<long> WriteAsync(
        ISequentialFile file,
        Schema schema,
        ChannelReader<RecordBatch> batches,
        CancellationToken cancellationToken = default)
    {
        ParquetWriteOptions options = EngineeredWoodParquetTableProperties.CreateWriteOptions(properties);
        await using ParquetFileWriter writer = new(file, ownsFile: false, options);
        Apache.Arrow.Schema arrowSchema = ArrowSchemas.FromSchema(schema);
        long written = 0;
        await foreach (RecordBatch batch in batches.ReadAllAsync(cancellationToken))
        {
            try
            {
                RecordBatch batchWithFieldIds = new(arrowSchema, batch.Arrays, batch.Length);
                await writer.WriteRowGroupAsync(batchWithFieldIds, cancellationToken);
                written += batch.Length;
            }
            finally
            {
                batch.Dispose();
            }
        }

        await writer.CloseAsync(cancellationToken);
        return written;
    }

    private static async ValueTask<IReadOnlyList<string>?> ResolveColumnsAsync(
        ParquetFileReader reader,
        IReadOnlySet<int>? fieldIds,
        CancellationToken cancellationToken)
    {
        if (fieldIds is null) return null;

        int[] requestedIds = fieldIds.Order().ToArray();
        SchemaDescriptor schema = await reader.GetSchemaAsync(cancellationToken);
        IReadOnlyList<string?> resolvedColumns = schema.ResolveFieldIds(requestedIds);
        int[] missingIds = requestedIds
            .Where((_, index) => resolvedColumns[index] is null)
            .ToArray();
        if (missingIds.Length != 0)
            throw new InvalidDataException(
                $"Parquet file does not contain requested field IDs {string.Join(", ", missingIds)}.");

        return resolvedColumns
            .Select(column => column!)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }
}