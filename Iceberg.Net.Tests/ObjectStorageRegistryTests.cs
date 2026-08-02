using Iceberg.Net.Rest;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Tests;

public class ObjectStorageRegistryTests
{
    [Fact]
    public async Task RoutesBySchemeAndAppliesMostSpecificCredential()
    {
        ObjectStorageRegistry.Register<TestObjectStorage>();
        var properties = new Dictionary<string, string> { ["source"] = "catalog", ["catalog-only"] = "value" };
        StorageCredential[] credentials =
        [
            new StorageCredential(
                new Dictionary<string, string> { ["source"] = "bucket" },
                "registry-test://bucket/"),
            new StorageCredential(
                new Dictionary<string, string> { ["source"] = "table", ["credential-only"] = "value" },
                "registry-test://bucket/table/")
        ];

        IObjectStorage router = ObjectStorageRegistry.CreateRouter(properties, credentials);
        await router.Open(
            new Uri("registry-test://bucket/table/data.parquet"),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("table", TestObjectStorage.LastProperties["source"]);
        Assert.Equal("value", TestObjectStorage.LastProperties["catalog-only"]);
        Assert.Equal("value", TestObjectStorage.LastProperties["credential-only"]);
    }

    private sealed class TestObjectStorage : IObjectStorage
    {
        public static IReadOnlyDictionary<string, string> LastProperties { get; private set; } =
            new Dictionary<string, string>();

        public static IReadOnlySet<string> Schemes { get; } = new HashSet<string> { "registry-test" };

        public static IObjectStorage Create(IReadOnlyDictionary<string, string> properties)
        {
            LastProperties = properties;
            return new TestObjectStorage();
        }

        public ValueTask<Stream> Open(
            Uri uri,
            FileMode fileMode = FileMode.Open,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult<Stream>(new MemoryStream());
        }
    }
}
