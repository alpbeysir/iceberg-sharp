using System.Threading.Channels;
using Apache.Arrow;
using Iceberg.Net.Metadata;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Catalog;

public sealed record AppendFilesOperation : ITableOperation
{
    public required Channel<RecordBatch> Data { get; init; }

    public required Schema Schema { get; init; }

    public PartitionSpec? PartitionSpec { get; init; }

    public SortOrder? SortOrder { get; init; }
}
