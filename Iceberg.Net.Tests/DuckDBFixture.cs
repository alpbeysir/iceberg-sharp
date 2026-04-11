using Iceberg.Net.DuckUtils;

namespace Iceberg.Net.Tests;

public sealed class DuckDbFixture : IAsyncLifetime
{
    public DuckDbCatalog DuckDbCatalog = new();

    public async ValueTask DisposeAsync()
    {
        await DuckDbCatalog.DisposeAsync();
    }

    public async ValueTask InitializeAsync()
    {
        await DuckDbCatalog.InitializeAsync();
    }
}