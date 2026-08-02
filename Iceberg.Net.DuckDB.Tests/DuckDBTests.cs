using System.Data.Common;
using System.Data.SqlTypes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Tests;

namespace Iceberg.Net.DuckDB.Tests;

public class DuckDBTests(DuckDbRestCatalogFixture restFixture, DuckDbFixture duckDbFixture)
    : ExternalEngineReadWriteTests(restFixture),
        IClassFixture<DuckDbRestCatalogFixture>,
        IClassFixture<DuckDbFixture>
{
    [Fact]
    public void DecimalNormalizationUsesNumericValue()
    {
        object? expected = TestRowNormalizer.ForDuckDb(SqlDecimal.Parse("93282.20"));
        object? actual = TestRowNormalizer.ForDuckDb(93282.2m);

        Assert.Equivalent(expected, actual, strict: true);
    }

    protected override object? NormalizeForExternalEngine(object? value) =>
        TestRowNormalizer.ForDuckDb(value);

    protected override async Task<List<object?>> ReadExternalTable(Identifier identifier)
    {
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

        return actual;
    }

    private static string QuoteIdentifier(string identifier) =>
        $"\"{identifier.Replace("\"", "\"\"")}\"";
}
