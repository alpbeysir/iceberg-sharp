using System.Threading.Channels;
using Apache.Arrow;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Tests;

public class DataFileFormatRegistryTests
{
    [Fact]
    public void ResolvesFormatsCaseInsensitivelyAndPassesTableProperties()
    {
        DataFileFormatRegistry.Register<TestDataFileFormat>();
        TablePropertyResolver properties = new(
            new Dictionary<string, string> { ["registry-test"] = "configured" });

        IDataFileFormat format = DataFileFormatRegistry.Resolve("REGISTRY-TEST", properties);

        Assert.IsType<TestDataFileFormat>(format);
        Assert.Equal("configured", ((TestDataFileFormat)format).ConfiguredValue);
    }

    [Fact]
    public void MissingFormatReportsTheUnregisteredName()
    {
        NotSupportedException exception = Assert.Throws<NotSupportedException>(
            () => DataFileFormatRegistry.Resolve(
                "not-registered",
                new TablePropertyResolver(null)));

        Assert.Contains("not-registered", exception.Message);
    }

    private sealed class TestDataFileFormat(TablePropertyResolver properties) : IDataFileFormat
    {
        public static IReadOnlySet<string> Formats { get; } =
            new HashSet<string> { "registry-test" };

        public static IDataFileFormat Create(TablePropertyResolver properties)
        {
            return new TestDataFileFormat(properties);
        }

        public string ConfiguredValue => properties.GetString("registry-test")!;

        public string Format => "registry-test";

        public string FileExtension => ".test";

        public Task ReadAsync(
            Stream stream,
            ChannelWriter<RecordBatch> results,
            CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public ValueTask<long> WriteAsync(
            Stream stream,
            Schema schema,
            ChannelReader<RecordBatch> batches,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(0L);
        }
    }
}
