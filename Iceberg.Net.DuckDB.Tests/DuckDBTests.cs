using System.Data.Common;
using System.Data.SqlTypes;
using Apache.Arrow.Serialization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Tests;
using Iceberg.Net.Tests.DataGeneration;

namespace Iceberg.Net.DuckDB.Tests;

public class DuckDBTests(DuckDbRestCatalogFixture restFixture, DuckDbFixture duckDbFixture)
    : TableTest(restFixture), IClassFixture<DuckDbRestCatalogFixture>, IClassFixture<DuckDbFixture>
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

    [Fact]
    public void DecimalNormalizationUsesNumericValue()
    {
        object? expected = TestRowNormalizer.ForDuckDb(SqlDecimal.Parse("93282.20"));
        object? actual = TestRowNormalizer.ForDuckDb(93282.2m);

        Assert.Equivalent(expected, actual, strict: true);
    }

    // [Theory]
    // [AutoIcebergData]
    // public async Task DeepNesting(List<MyDeeplyNestedComplexRow> rows)
    // {
    //     await Run(rows);
    // }

    private async Task Run<T>(List<T> original) where T : IArrowSerializer<T>
    {
        Identifier identifier = await Write(original);
        await Verify(identifier, original);
        string tableName = string.Join(
            ".",
            new[] { duckDbFixture.DuckDbCatalog.CatalogName }
                .Concat(identifier)
                .Select(QuoteIdentifier));
        await using DbDataReader reader =
            await duckDbFixture.DuckDbCatalog
                .ExecuteQuery($"SELECT * FROM {tableName};");

        var actual = new List<object?>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var row = new Dictionary<object, object?>();
            for (int column = 0; column < reader.FieldCount; column++)
                row[reader.GetName(column)] = TestRowNormalizer.ForDuckDb(reader.GetValue(column));
            actual.Add(row);
        }

        List<object?> expected = original.Select(row => TestRowNormalizer.ForDuckDb(row)).ToList();
        Assert.Equivalent(expected, actual, strict: true);
    }

    private static string QuoteIdentifier(string identifier) =>
        $"\"{identifier.Replace("\"", "\"\"")}\"";
}
