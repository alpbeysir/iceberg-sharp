using Apache.Arrow.Serialization;
using AwesomeAssertions;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.Tests;

public class TableTest(RestCatalogFixture fixture)
{
    private readonly ICatalog Catalog = fixture.GetCatalog();

    protected async Task<Identifier> Write<T>(List<T> rows)
    {
        var identifier = GetTableName<T>();

        await using var transaction = new Transaction(new Table(identifier, Catalog));
        await transaction.AppendRows(rows, TestContext.Current.CancellationToken);
        await transaction.Commit(TestContext.Current.CancellationToken);

        return identifier;
    }

    protected async Task Verify<T>(Identifier identifier, List<T> original) where T : IArrowSerializer<T>
    {
        var loadedTable = await Catalog.LoadTableAsync(identifier);
        await using var readTx = new Transaction(loadedTable);
        var readRows = readTx.ReadRows<T>().ToList();
        readRows.Should().BeEquivalentTo(original, opt => opt.WithStrictOrdering());
    }

    private Identifier GetTableName<T>()
    {
        var tableName =
            $"{TestContext.Current.Test?.TestCase?.TestMethod?.MethodName}";
        return [.. fixture.BaseNamespace, tableName];
    }
}