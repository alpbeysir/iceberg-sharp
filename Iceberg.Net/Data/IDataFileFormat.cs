using Apache.Arrow;
using EngineeredWood.IO;
using Iceberg.Net.Catalog;
using Iceberg.Net.Storage;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Data;

public interface IDataFileFormat
{
    static virtual IReadOnlySet<string> Formats { get; } = new HashSet<string>();

    static abstract IDataFileFormat Create(
        TablePropertyResolver properties,
        ILoggerFactory loggerFactory);

    string Format { get; }

    string FileExtension { get; }

    Task ReadAsync(
        Stream stream,
        Schema schema,
        ChannelWriter<RecordBatch> results,
        IReadOnlySet<int>? fieldIds = null,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(
            $"{GetType().FullName} does not support reading from a stream.");

    async Task ReadAsync(
        IRandomAccessFile file,
        Schema schema,
        ChannelWriter<RecordBatch> results,
        IReadOnlySet<int>? fieldIds = null,
        CancellationToken cancellationToken = default)
    {
        await using RandomAccessFileStream stream = new(file);
        await ReadAsync(stream, schema, results, fieldIds, cancellationToken);
    }

    ValueTask<long> WriteAsync(
        Stream stream,
        Schema schema,
        ChannelReader<RecordBatch> batches,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(
            $"{GetType().FullName} does not support writing to a stream.");

    async ValueTask<long> WriteAsync(
        ISequentialFile file,
        Schema schema,
        ChannelReader<RecordBatch> batches,
        CancellationToken cancellationToken = default)
    {
        await using SequentialFileStream stream = new(file);
        return await WriteAsync(stream, schema, batches, cancellationToken);
    }
}
