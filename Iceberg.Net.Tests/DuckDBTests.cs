using Apache.Arrow.Serialization;
using Iceberg.Net.Tests.DataGeneration;

namespace Iceberg.Net.Tests;

public class DuckDBTests(RestCatalogFixture restFixture, DuckDbFixture duckDbFixture)
    : TableTest(restFixture), IClassFixture<DuckDbFixture>
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

    // [Theory]
    // [AutoIcebergData]
    // public async Task DeepNesting(List<MyDeeplyNestedComplexRow> rows)
    // {
    //     await Run(rows);
    // }

    private async Task Run<T>(List<T> original) where T : IArrowSerializer<T>
    {
        var identifier = await Write(original);
        await Verify(identifier, original);
        var reader =
            await duckDbFixture.DuckDbCatalog
                .ExecuteQuery($"SELECT * FROM {duckDbFixture.DuckDbCatalog.CatalogName}.{identifier};");
        Assert.True(reader.HasRows);
    }
}