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

    Task ReadAsync(
        Stream stream,
        Schema schema,
        ChannelWriter<RecordBatch> results,
        IReadOnlySet<int>? fieldIds = null,
        CancellationToken cancellationToken = default);

    ValueTask<long> WriteAsync(
        Stream stream,
        Schema schema,
        ChannelReader<RecordBatch> batches,
        CancellationToken cancellationToken = default);
}
