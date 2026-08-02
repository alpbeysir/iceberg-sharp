using Iceberg.Net.Catalog;
using Iceberg.Net.S3;
using Iceberg.Net.Storage;
using Iceberg.Net.Tests;

[assembly: AssemblyFixture(typeof(RestCatalogFixture))]

namespace Iceberg.Net.Tests;

public sealed class RestCatalogFixture : IAsyncLifetime
{
    private const bool DropAfter = true;
    private ICatalog? _catalog;

    public Identifier BaseNamespace => ["test"];

    public async ValueTask InitializeAsync()
    {
        ObjectStorageRegistry.Register<S3ObjectStorage>();
        S3Config storageConfig = new()
        {
            Endpoint = "http://127.0.0.1:8333",
            AccessKeyId = "admin",
            SecretAccessKey = "key",
            ForcePathStyle = true
        };
        UserConfig userConfig = new UserConfig { BaseUrl = "http://localhost:8181/v1" };
        foreach (KeyValuePair<string, string> property in storageConfig.ToProperties())
            userConfig.CatalogConfig[property.Key] = property.Value;
        _catalog = await RestCatalog.Create(userConfig);

        if (await _catalog.NamespaceExistsAsync(BaseNamespace)) await _catalog.DropNamespaceAsync(BaseNamespace, true);
        await _catalog.CreateNamespaceIfNotExistsAsync(BaseNamespace);
    }

    public async ValueTask DisposeAsync()
    {
        if (_catalog is null) return;

        if (DropAfter) await _catalog.DropNamespaceAsync(BaseNamespace, true);
        _catalog.Dispose();
    }

    public ICatalog GetCatalog()
    {
        return _catalog!;
    }
}
