using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
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

    public async Task ReadAsync(
        Stream stream,
        ChannelWriter<RecordBatch> results,
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
        using IArrowArrayStream recordBatchReader = arrowReader.GetRecordBatchReader();

        while (!cancellationToken.IsCancellationRequested)
        {
            RecordBatch? batch = await recordBatchReader.ReadNextRecordBatchAsync(cancellationToken);
            if (batch is null) break;
            await results.WriteAsync(batch, cancellationToken);
        }
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