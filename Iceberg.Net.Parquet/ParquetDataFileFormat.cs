using System.Linq.Expressions;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Schemas;
using ParquetSharp;
using ParquetSharp.Arrow;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Parquet;

public sealed class ParquetDataFileFormat(TablePropertyResolver properties) : IDataFileFormat
{
    public static IReadOnlySet<string> Formats { get; } =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "parquet" };

    public static IDataFileFormat Create(TablePropertyResolver properties)
    {
        return new ParquetDataFileFormat(properties);
    }

    public string Format => "parquet";

    public string FileExtension => ".parquet";

    public async Task ReadAsync(Stream stream,
        Schema schema,
        ChannelWriter<RecordBatch> results,
        IReadOnlySet<int>? fieldIds = null,
        CancellationToken cancellationToken = default)
    {
        using ArrowReaderProperties arrowReaderProperties = ArrowReaderProperties.GetDefault();
        ParquetTableProperties.ApplyReaderProperties(arrowReaderProperties, properties);
        using ReaderProperties parquetReaderProperties = ReaderProperties.GetDefaultReaderProperties();
        using FileReader arrowReader = new(
            stream,
            parquetReaderProperties,
            arrowReaderProperties,
            leaveOpen: true);

        using IArrowArrayStream recordBatchReader = fieldIds is null
            ? arrowReader.GetRecordBatchReader()
            : arrowReader.GetRecordBatchReader(
                Enumerable.Range(0, arrowReader.NumRowGroups).ToArray(),
                ResolveColumnIndices(arrowReader.SchemaManifest, fieldIds));

        while (!cancellationToken.IsCancellationRequested)
        {
            RecordBatch? batch = await recordBatchReader.ReadNextRecordBatchAsync(cancellationToken);
            if (batch is null) break;
            await PipelineMetrics.WriteAsync(
                results,
                batch,
                PipelineStage.DataFileRead,
                cancellationToken);
        }
    }

    private static int[] ResolveColumnIndices(
        SchemaManifest manifest,
        IReadOnlySet<int> fieldIds)
    {
        HashSet<int> columns = [];
        HashSet<int> foundFieldIds = [];
        foreach (SchemaField field in manifest.SchemaFields)
            CollectColumns(field, fieldIds, false, columns, foundFieldIds);

        int[] missing = fieldIds.Except(foundFieldIds).Order().ToArray();
        if (missing.Length != 0)
            throw new InvalidDataException(
                $"Parquet file does not contain requested field IDs {string.Join(", ", missing)}.");

        return columns.Order().ToArray();
    }

    private static void CollectColumns(
        SchemaField field,
        IReadOnlySet<int> requestedFieldIds,
        bool parentSelected,
        ISet<int> columns,
        ISet<int> foundFieldIds)
    {
        bool selected = parentSelected;
        if (TryGetFieldId(field.Field, out int fieldId) && requestedFieldIds.Contains(fieldId))
        {
            selected = true;
            foundFieldIds.Add(fieldId);
        }

        foreach (SchemaField child in field.Children)
            CollectColumns(child, requestedFieldIds, selected, columns, foundFieldIds);

        if (selected && field.ColumnIndex >= 0) columns.Add(field.ColumnIndex);
    }

    private static bool TryGetFieldId(Field field, out int fieldId)
    {
        fieldId = default;
        return field.Metadata is not null &&
               field.Metadata.TryGetValue("PARQUET:field_id", out string? value) &&
               int.TryParse(value, out fieldId);
    }

    public async ValueTask<long> WriteAsync(
        Stream stream,
        Schema schema,
        ChannelReader<RecordBatch> batches,
        CancellationToken cancellationToken = default)
    {
        using WriterProperties parquetWriterProperties =
            ParquetTableProperties.CreateWriterProperties(properties);
        using ArrowWriterProperties arrowWriterProperties = ArrowWriterProperties.GetDefault();
        Apache.Arrow.Schema arrowSchema = ArrowSchemas.FromSchema(schema);
        using FileWriter arrowWriter = new(
            stream,
            arrowSchema,
            parquetWriterProperties,
            arrowWriterProperties,
            leaveOpen: true);
        long written = 0;
        await foreach (RecordBatch batch in batches.ReadAllAsync(cancellationToken))
        {
            using (batch)
            {
                // TODO for now have to clone due to Arrow limitations
                arrowWriter.WriteBufferedRecordBatch(batch.Clone());
                written += batch.Length;
            }
        }

        arrowWriter.Close();
        return written;
    }
}
