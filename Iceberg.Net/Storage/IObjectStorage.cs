namespace Iceberg.Net.Storage;

public interface IObjectStorage
{
    public ValueTask<Stream> Open(
        Uri uri,
        FileMode fileMode = FileMode.Open,
        CancellationToken cancellationToken = default);
}

public readonly record struct PathAndStream(Uri Path, Stream Stream) : IDisposable, IAsyncDisposable
{
    public async ValueTask DisposeAsync()
    {
        await Stream.DisposeAsync();
    }

    public void Dispose()
    {
        Stream.Dispose();
    }
}