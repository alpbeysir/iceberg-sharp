using Apache.Arrow.Serialization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Tests;
using Iceberg.Net.Tests.DataGeneration;

namespace Iceberg.Net.Spark.Tests;

public class PySparkTests(SparkRestCatalogFixture restFixture, PySparkFixture pySparkFixture)
    : TableTest(restFixture), IClassFixture<SparkRestCatalogFixture>, IClassFixture<PySparkFixture>
{
    [Theory]
    [AutoIcebergData]
    public async Task SimpleNesting(List<MyNested> rows)
    {
        await Run(rows);
    }

    [Theory]
    [AutoIcebergData]
    public async Task Simple(List<MySimpleRow> rows)
    {
        await Run(rows);
    }

    [Theory]
    [AutoIcebergData]
    public async Task NestedComplexRow(List<NestedComplexRow> rows)
    {
        await Run(rows);
    }

    [Theory]
    [AutoIcebergData]
    public async Task ManyTypes(List<ManyTypes> rows)
    {
        await Run(rows);
    }

    private async Task Run<T>(List<T> original) where T : IArrowSerializer<T>
    {
        Identifier identifier = await Write(original);
        await Verify(identifier, original);

        List<object?> actual = await pySparkFixture.ReadTable(identifier);
        List<object?> expected = original.Select(row => TestRowNormalizer.ForPySpark(row)).ToList();

        Assert.Equivalent(expected, actual, strict: true);
    }
}
