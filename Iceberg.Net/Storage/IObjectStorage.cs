namespace Iceberg.Net.Storage;

public delegate string? PropertyResolver(string key);

public interface IObjectStorage
{
    static virtual IReadOnlySet<string> Schemes { get; } = new HashSet<string>();

    static virtual IObjectStorage Create(PropertyResolver resolve)
    {
        throw new NotSupportedException("The object storage implementation does not provide a static factory");
    }

    ValueTask<Stream> Open(
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
