using Apache.Arrow.Serialization;
using AwesomeAssertions;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.Tests;

public class TableTest(RestCatalogFixture fixture)
{
    protected readonly ICatalog Catalog = fixture.GetCatalog();

    protected async Task<Identifier> Write<T>(List<T> rows)
    {
        Identifier identifier = GetTableName();

        TableOperations tableOperations = await Catalog.OperationsAsync(
            identifier,
            TestContext.Current.CancellationToken);
        await tableOperations.FastAppendRows(rows, TestContext.Current.CancellationToken);

        return identifier;
    }

    protected async Task Verify<T>(Identifier identifier, List<T> original) where T : IArrowSerializer<T>
    {
        Table loadedTable = await Catalog.LoadTableAsync(identifier) ??
                            throw new InvalidOperationException($"Table '{identifier}' was not found.");
        TableOperations operations = loadedTable.Operations();
        var readRows = operations.ReadRows<T>().ToList();
        readRows.Should()
            .BeEquivalentTo(
                original,
                options => options
                    .Using<DateTime>(context => context.Subject
                        .Should()
                        .BeCloseTo(
                            context.Expectation,
                            TimeSpan.FromMicroseconds(1)))
                    .WhenTypeIs<DateTime>()
                    .WithStrictOrdering());
    }

    protected Identifier GetTableName()
    {
        string tableName =
            $"{GetType().Name}_{TestContext.Current.Test?.TestCase?.TestMethod?.MethodName}";
        return [.. fixture.BaseNamespace, tableName];
    }
}
