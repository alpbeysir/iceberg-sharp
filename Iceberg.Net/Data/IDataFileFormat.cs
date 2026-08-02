using Apache.Arrow;
using Iceberg.Net.Catalog;
using System.Threading.Channels;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Data;

public interface IDataFileFormat
{
    static virtual IReadOnlySet<string> Formats { get; } = new HashSet<string>();

    static virtual IDataFileFormat Create(TablePropertyResolver properties)
    {
        throw new NotSupportedException("The data file format implementation does not provide a static factory");
    }

    string Format { get; }

    string FileExtension { get; }

    /// <summary>
    /// Reads record batches into a potentially shared channel. Implementations must not complete
    /// <paramref name="results"/> because other data files may still be producing batches.
    /// </summary>
    Task ReadAsync(
        Stream stream,
        ChannelWriter<RecordBatch> results,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes batches supplied by the caller. Implementations consume and dispose each batch, but
    /// do not own or complete the channel.
    /// </summary>
    ValueTask<long> WriteAsync(
        Stream stream,
        Schema schema,
        ChannelReader<RecordBatch> batches,
        CancellationToken cancellationToken = default);
}
