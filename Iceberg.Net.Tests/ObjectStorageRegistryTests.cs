using Iceberg.Net.Rest;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Tests;

public class ObjectStorageRegistryTests
{
    [Fact]
    public async Task ResolvesBySchemeAndAppliesMostSpecificCredential()
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

        var uri = new Uri("registry-test://bucket/table/data.parquet");
        IObjectStorage storage = ObjectStorageRegistry.Resolve(
            uri,
            key => properties.TryGetValue(key, out string? value) ? value : null,
            credentials);
        await storage.Open(
            uri,
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

        public static IObjectStorage Create(PropertyResolver resolve)
        {
            LastProperties = new Dictionary<string, string>
            {
                ["source"] = resolve("source")!,
                ["catalog-only"] = resolve("catalog-only")!,
                ["credential-only"] = resolve("credential-only")!
            };
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
