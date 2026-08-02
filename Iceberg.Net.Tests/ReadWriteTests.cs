using Apache.Arrow.Serialization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Tests.DataGeneration;

namespace Iceberg.Net.Tests;

public class ReadWriteTests(RestCatalogFixture fixture) : TableTest(fixture)
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
    public async Task SqlDecimals()
    {
        await Run(DecimalRow.TestRows().ToList());
    }

    // [Theory]
    // [AutoIcebergData]
    // public async Task DeepNesting(List<MyDeeplyNestedComplexRow> rows)
    // {
    //     await Run(rows);
    // }

    private async Task Run<T>(List<T> rows) where T : IArrowSerializer<T>
    {
        Identifier identifier = await Write(rows);
        await Verify(identifier, rows);
    }
}
