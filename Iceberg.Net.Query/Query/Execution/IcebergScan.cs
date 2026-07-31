using System.Threading.Channels;

namespace Iceberg.Net.Query.Execution;

internal class IcebergScan
{
    public Task ExecuteAsync(
        Channel<ColumnBufferSet> outputs,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }
}