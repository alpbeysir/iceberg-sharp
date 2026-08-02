using EngineeredWood.IO;
using Iceberg.Net.Rest;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Tests;

public class TableFileSystemRegistryTests
{
    [Fact]
    public void ResolvesBySchemeAndAppliesMostSpecificCredential()
    {
        TableFileSystemRegistry.Register<TestTableFileSystemFactory>();
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

        Uri uri = new("registry-test://bucket/table/data.parquet");
        ITableFileSystem fileSystem = TableFileSystemRegistry.Resolve(
            uri,
            key => properties.TryGetValue(key, out string? value) ? value : null,
            credentials);

        Assert.IsType<TestTableFileSystem>(fileSystem);
        Assert.Same(uri, TestTableFileSystemFactory.LastUri);
        Assert.Equal("table", TestTableFileSystemFactory.LastProperties["source"]);
        Assert.Equal("value", TestTableFileSystemFactory.LastProperties["catalog-only"]);
        Assert.Equal("value", TestTableFileSystemFactory.LastProperties["credential-only"]);
    }

    private sealed class TestTableFileSystemFactory : ITableFileSystemFactory
    {
        public static IReadOnlyDictionary<string, string> LastProperties { get; private set; } =
            new Dictionary<string, string>();

        public static Uri? LastUri { get; private set; }

        public static IReadOnlySet<string> Schemes { get; } = new HashSet<string> { "registry-test" };

        public static ITableFileSystem Create(Uri uri, PropertyResolver resolve)
        {
            LastUri = uri;
            LastProperties = new Dictionary<string, string>
            {
                ["source"] = resolve("source")!,
                ["catalog-only"] = resolve("catalog-only")!,
                ["credential-only"] = resolve("credential-only")!
            };
            return new TestTableFileSystem();
        }
    }

    private sealed class TestTableFileSystem : ITableFileSystem
    {
        public IAsyncEnumerable<TableFileInfo> ListAsync(
            string prefix,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<IRandomAccessFile> OpenReadAsync(
            string path,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<ISequentialFile> CreateAsync(
            string path,
            bool overwrite = false,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<bool> RenameAsync(
            string sourcePath,
            string targetPath,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask DeleteAsync(
            string path,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<bool> ExistsAsync(
            string path,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<byte[]> ReadAllBytesAsync(
            string path,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask WriteAllBytesAsync(
            string path,
            ReadOnlyMemory<byte> data,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
