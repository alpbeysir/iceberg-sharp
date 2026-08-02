using System.Collections.Immutable;
using System.Threading.Channels;
using EngineeredWood.Expressions;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Tests;

namespace Iceberg.Net.Spark.Tests;

public class PySparkTests(SparkRestCatalogFixture restFixture, PySparkFixture pySparkFixture)
    : ExternalEngineReadWriteTests(restFixture),
        IClassFixture<SparkRestCatalogFixture>,
        IClassFixture<PySparkFixture>
{
    [Fact]
    public async Task PartitionValuesAreReadAsLiterals()
    {
        Identifier identifier = GetTableName();
        (long Id, string Category, long Region)[] rows =
        [
            (1, "alpha", 10),
            (2, "alpha", 10),
            (3, "beta", 20),
            (4, "gamma", 20)
        ];
        ImmutableArray<LiteralValue?>[] expectedPartitions =
        [
            [LiteralValue.Of("alpha"), LiteralValue.Of(10L)],
            [LiteralValue.Of("beta"), LiteralValue.Of(20L)],
            [LiteralValue.Of("gamma"), LiteralValue.Of(20L)]
        ];

        await pySparkFixture.WritePartitionedTable(identifier, rows);

        Table table = await Catalog.LoadTableAsync(
                          identifier,
                          cancellationToken: TestContext.Current.CancellationToken) ??
                      throw new InvalidOperationException($"Table '{identifier}' was not found.");
        PartitionSpec spec = Assert.Single(table.Metadata.PartitionSpecs);
        Assert.Equal(["category", "region"], spec.Fields.Select(field => field.Name));

        Channel<ManifestEntry> entries = Channel.CreateUnbounded<ManifestEntry>();
        await table.Operations().ReadManifestEntries(
            entries.Writer,
            cancellationToken: TestContext.Current.CancellationToken);
        entries.Writer.Complete();

        List<DataFile> dataFiles = [];
        while (entries.Reader.TryRead(out ManifestEntry entry))
            if (entry.Status != Status.Deleted)
                dataFiles.Add(entry.DataFile);

        Assert.NotEmpty(dataFiles);
        foreach (DataFile dataFile in dataFiles)
        {
            Assert.Equal(2, dataFile.Partition.Length);
            Assert.Equal(LiteralValue.Kind.String, dataFile.Partition[0]!.Value.Type);
            Assert.Equal(LiteralValue.Kind.Int64, dataFile.Partition[1]!.Value.Type);
            Assert.Contains(
                expectedPartitions,
                expected => expected.SequenceEqual(dataFile.Partition));
        }

        foreach (ImmutableArray<LiteralValue?> expected in expectedPartitions)
            Assert.Contains(dataFiles, file => expected.SequenceEqual(file.Partition));
    }

    protected override Task<List<object?>> ReadExternalTable(Identifier identifier) =>
        pySparkFixture.ReadTable(identifier);

    protected override object? NormalizeForExternalEngine(object? value) =>
        TestRowNormalizer.ForPySpark(value);
}
