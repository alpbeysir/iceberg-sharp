using Apache.Arrow.Serialization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Schemas;
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

    [Fact]
    public async Task ReadRejectsRowTypeThatDoesNotMatchSnapshotSchema()
    {
        Identifier identifier = await Write([
            new ReadSchemaRow { Id = 1, Name = "one" }
        ]);
        Table table = await Catalog.LoadTableAsync(
                          identifier,
                          cancellationToken: TestContext.Current.CancellationToken) ??
                      throw new InvalidOperationException($"Table '{identifier}' was not found.");

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
            table.Operations().ReadRows<MismatchedReadSchemaRow>().ToList());

        Assert.Contains(nameof(MismatchedReadSchemaRow.Extra), exception.Message);
        Assert.Contains("snapshot", exception.Message);
    }

    [Theory]
    [AutoIcebergData]
    public async Task ParquetTablePropertiesAreApplied(List<MySimpleRow> rows)
    {
        Identifier identifier = GetTableName();
        int nextFieldId = 1;
        Schema schema = CSharpSchemas.ToIcebergSchema(typeof(MySimpleRow), 0, _ => nextFieldId++);
        var properties = new Dictionary<string, string>
        {
            [TableProperties.DefaultFileFormat] = "parquet",
            [TableProperties.ParquetCompression] = "gzip",
            [TableProperties.ParquetCompressionLevel] = "1",
            [TableProperties.ManifestCompression] = "gzip",
            [TableProperties.ManifestCompressionLevel] = "1",
            [TableProperties.ParquetPageSizeBytes] = "65536",
            [TableProperties.ParquetPageVersion] = "v2",
            [TableProperties.ParquetDictSizeBytes] = "32768",
            [TableProperties.ParquetDictEncodingEnabledColumnPrefix + nameof(MySimpleRow.Str)] = "false",
            [TableProperties.ParquetColumnStatsEnabledPrefix + nameof(MySimpleRow.Num)] = "false",
            [TableProperties.ParquetBatchSize] = "2"
        };
        Table table = await Catalog.CreateTableAsync(
            identifier,
            schema,
            properties,
            TestContext.Current.CancellationToken);

        TableOperations tableOperations = table.Operations();
        await tableOperations.FastAppendRowsAot(rows, TestContext.Current.CancellationToken);

        Table updatedTable = await Catalog.LoadTableAsync(
                                 identifier,
                                 cancellationToken: TestContext.Current.CancellationToken) ??
                             throw new InvalidOperationException($"Table '{identifier}' was not found.");
        Snapshot snapshot = updatedTable.Metadata.SnapshotsById[
            updatedTable.Metadata.CurrentSnapshotId!.Value];
        Assert.Equal(1, snapshot.Summary.AddedDataFiles);
        Assert.Equal(rows.Count, snapshot.Summary.AddedRecords);
        Assert.True(snapshot.Summary.AddedFilesSize > 0);

        await Verify(identifier, rows);
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

[ArrowSerializable]
public partial record ReadSchemaRow
{
    public int Id { get; init; }
    public string Name { get; init; } = string.Empty;
}

[ArrowSerializable]
public partial record MismatchedReadSchemaRow
{
    public int Id { get; init; }
    public string Name { get; init; } = string.Empty;
    public long Extra { get; init; }
}
