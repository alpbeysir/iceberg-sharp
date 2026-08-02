using Apache.Arrow.Serialization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Tests.DataGeneration;

namespace Iceberg.Net.Tests;

public abstract class ExternalEngineReadWriteTests(RestCatalogFixture fixture) : TableTest(fixture)
{
    private const int RepeatedAppendCount = 10;

    protected abstract Task<List<object?>> ReadExternalTable(Identifier identifier);

    protected abstract object? NormalizeForExternalEngine(object? value);

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

    [Theory]
    [AutoIcebergData]
    public async Task SimpleRowsCanBeAppendedManyTimes(List<MySimpleRow> rows)
    {
        await RunRepeatedAppends(rows);
    }

    [Theory]
    [AutoIcebergData]
    public async Task NestedComplexRowsCanBeAppendedManyTimes(List<NestedComplexRow> rows)
    {
        await RunRepeatedAppends(rows);
    }

    private async Task Run<T>(List<T> original) where T : IArrowSerializer<T>
    {
        Identifier identifier = await Write(original);
        await Verify(identifier, original);
        await VerifyExternalRows(identifier, original);
    }

    private async Task RunRepeatedAppends<T>(List<T> rows) where T : IArrowSerializer<T>
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        Identifier identifier = GetTableName();
        TableOperations operations = await Catalog.OperationsAsync(identifier, cancellationToken);
        var expected = new List<T>(rows.Count * RepeatedAppendCount);

        for (int append = 0; append < RepeatedAppendCount; append++)
        {
            await operations.FastAppendRowsAot(rows, cancellationToken);
            expected.AddRange(rows);
        }

        Table table = await Catalog.LoadTableAsync(identifier, cancellationToken: cancellationToken) ??
                      throw new InvalidOperationException($"Table '{identifier}' was not found.");
        Assert.Equal(RepeatedAppendCount, table.Metadata.Snapshots.Count);
        await VerifyExternalRows(identifier, expected);
    }

    private async Task VerifyExternalRows<T>(Identifier identifier, List<T> original)
    {
        List<object?> actual = await ReadExternalTable(identifier);
        List<object?> expected = original.Select(row => NormalizeForExternalEngine(row)).ToList();

        Assert.Equivalent(expected, actual, strict: true);
    }
}
