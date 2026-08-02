using EngineeredWood.IO;
using EngineeredWood.IO.Aws;
using EngineeredWood.IO.Azure;
using EngineeredWood.IO.Gcs;
using Iceberg.Net.Azure;
using Iceberg.Net.GCS;
using Iceberg.Net.S3;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Tests;

public class TableFileSystemProviderTests
{
    [Fact]
    public void AdlsConfigPrefersAccountScopedProperties()
    {
        Dictionary<string, string> properties = new()
        {
            ["adls.connection-string"] = "https://fallback.example.test",
            ["adls.connection-string.account"] = "https://account.example.test",
            ["adls.sas-token"] = "fallback-token",
            ["adls.sas-token.account"] = "account-token",
            ["adls.auth.shared-key.account.name"] = "account",
            ["adls.auth.shared-key.account.key"] = "shared-key"
        };

        ADLSConfig config = ADLSConfig.FromResolver("account", Resolver(properties));

        Assert.Equal("https://account.example.test", config.ConnectionString);
        Assert.Equal("account-token", config.SasToken);
        Assert.Equal("account", config.SharedKeyAccountName);
        Assert.Equal("shared-key", config.SharedKeyAccountKey);
    }

    [Fact]
    public void GcsConfigResolvesProperties()
    {
        Dictionary<string, string> properties = new()
        {
            ["gcs.oauth2.token"] = "access-token",
            ["gcs.no-auth"] = "true",
            ["gcs.service.host"] = "http://localhost:4443"
        };

        GCSConfig config = GCSConfig.FromResolver(Resolver(properties));

        Assert.Equal("access-token", config.AccessToken);
        Assert.True(config.NoAuth);
        Assert.Equal("http://localhost:4443", config.ServiceHost);
    }

    [Fact]
    public void S3ProviderUsesEngineeredWoodPackage()
    {
        Dictionary<string, string> properties = new()
        {
            ["s3.endpoint"] = "http://localhost:9000",
            ["s3.access-key-id"] = "access-key",
            ["s3.secret-access-key"] = "secret-key",
            ["s3.path-style-access"] = "true"
        };

        ITableFileSystem fileSystem = Resolve(
            new Uri("s3://warehouse/table/data/file.parquet"),
            properties);

        Assert.IsType<S3TableFileSystem>(fileSystem);
    }

    [Theory]
    [InlineData("abfs://container@account.dfs.core.windows.net/table/data/file.parquet")]
    [InlineData("abfss://container@account.dfs.core.windows.net/table/data/file.parquet")]
    [InlineData("wasb://container@account.blob.core.windows.net/table/data/file.parquet")]
    [InlineData("wasbs://container@account.blob.core.windows.net/table/data/file.parquet")]
    public void AzureProviderUsesEngineeredWoodPackage(string location)
    {
        Dictionary<string, string> properties = new()
        {
            ["adls.sas-token.account"] = "sv=2025-01-05&sig=test"
        };

        ITableFileSystem fileSystem = Resolve(new Uri(location), properties);

        Assert.IsType<AzureTableFileSystem>(fileSystem);
    }

    [Theory]
    [InlineData("gs://warehouse/table/data/file.parquet")]
    [InlineData("gcs://warehouse/table/data/file.parquet")]
    public void GcsProviderUsesEngineeredWoodPackage(string location)
    {
        Dictionary<string, string> properties = new() { ["gcs.no-auth"] = "true" };

        ITableFileSystem fileSystem = Resolve(new Uri(location), properties);

        Assert.IsType<GcsTableFileSystem>(fileSystem);
    }

    private static ITableFileSystem Resolve(Uri uri, IReadOnlyDictionary<string, string> properties)
    {
        TableFileSystemRegistry.Register<S3TableFileSystemFactory>();
        TableFileSystemRegistry.Register<AzureTableFileSystemFactory>();
        TableFileSystemRegistry.Register<GcsTableFileSystemFactory>();
        return TableFileSystemRegistry.Resolve(uri, Resolver(properties));
    }

    private static PropertyResolver Resolver(IReadOnlyDictionary<string, string> properties) =>
        key => properties.TryGetValue(key, out string? value) ? value : null;
}
