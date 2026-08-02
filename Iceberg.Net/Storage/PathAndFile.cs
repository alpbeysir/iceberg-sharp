namespace Iceberg.Net.Storage;

public readonly record struct PathAndFile<TFile>(Uri Path, TFile File) : IDisposable, IAsyncDisposable
    where TFile : IDisposable, IAsyncDisposable
{
    public ValueTask DisposeAsync() => File.DisposeAsync();

    public void Dispose() => File.Dispose();
}
