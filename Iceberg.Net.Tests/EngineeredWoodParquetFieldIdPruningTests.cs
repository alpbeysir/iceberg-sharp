using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using EngineeredWood.IO.Local;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.EngineeredWoodParquet;
using Iceberg.Net.Schemas;
using IcebergSchema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Tests;

public class EngineeredWoodParquetFieldIdPruningTests
{
    [Fact]
    public async Task RoundTripsAndProjectsColumnsByFieldId()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"iceberg-engineered-wood-{Guid.NewGuid():N}.parquet");
        try
        {
            IcebergSchema schema = new(
            [
                new StructField(10, "id", new PrimitiveType.Int(), true),
                new StructField(20, "name", new PrimitiveType.String(), true)
            ]);
            Apache.Arrow.Schema arrowSchema = ArrowSchemas.FromSchema(schema);
            RecordBatch inputBatch = new(
                arrowSchema,
                [
                    new Int32Array.Builder().Append(1).Append(2).Build(),
                    new StringArray.Builder().Append("one").Append("two").Build()
                ],
                2);
            Channel<RecordBatch> input = Channel.CreateUnbounded<RecordBatch>();
            await input.Writer.WriteAsync(inputBatch, cancellationToken);
            input.Writer.Complete();

            EngineeredWoodParquetDataFileFormat format = new(new TablePropertyResolver(null));
            await using (LocalSequentialFile file = new(path))
                Assert.Equal(
                    2,
                    await format.WriteAsync(file, schema, input.Reader, cancellationToken));

            Channel<RecordBatch> output = Channel.CreateUnbounded<RecordBatch>();
            await using (LocalRandomAccessFile file = new(path))
                await format.ReadAsync(
                    file,
                    schema,
                    output.Writer,
                    new HashSet<int> { 20 },
                    cancellationToken);
            output.Writer.Complete();

            using RecordBatch projected = await output.Reader.ReadAsync(cancellationToken);
            Field field = Assert.Single(projected.Schema.FieldsList);
            Assert.Equal("name", field.Name);
            StringArray names = Assert.IsType<StringArray>(Assert.Single(projected.Arrays));
            Assert.Equal("one", names.GetString(0));
            Assert.Equal("two", names.GetString(1));
            Assert.False(output.Reader.TryRead(out _));

            IDataFileFormat parquetSharpFormat =
                new Iceberg.Net.Parquet.ParquetDataFileFormat(new TablePropertyResolver(null));
            Channel<RecordBatch> parquetSharpOutput = Channel.CreateUnbounded<RecordBatch>();
            await using (LocalRandomAccessFile file = new(path))
                await parquetSharpFormat.ReadAsync(
                    file,
                    schema,
                    parquetSharpOutput.Writer,
                    cancellationToken: cancellationToken);
            parquetSharpOutput.Writer.Complete();

            using RecordBatch parquetSharpBatch =
                await parquetSharpOutput.Reader.ReadAsync(cancellationToken);
            Assert.Equal(2, parquetSharpBatch.ColumnCount);
            Assert.Equal(1, Assert.IsType<Int32Array>(parquetSharpBatch.Column(0)).GetValue(0));
            Assert.Equal("two", Assert.IsType<StringArray>(parquetSharpBatch.Column(1)).GetString(1));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTripsNestedLists()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"iceberg-engineered-wood-nested-{Guid.NewGuid():N}.parquet");
        try
        {
            NestedListRow[] rows = Enumerable.Range(0, 10_000)
                .Select(value => new NestedListRow
                {
                    Values = [value, value + 1, value + 2],
                    NestedValues = [[1, value + 3], [1, 5]]
                })
                .ToArray();
            int nextFieldId = 1;
            IcebergSchema schema = CSharpSchemas.ToIcebergSchema(
                typeof(NestedListRow),
                0,
                _ => nextFieldId++);
            Channel<RecordBatch> input = Channel.CreateUnbounded<RecordBatch>();
            await input.Writer.WriteAsync(NestedListRow.ToRecordBatch(rows), cancellationToken);
            input.Writer.Complete();

            EngineeredWoodParquetDataFileFormat format = new(new TablePropertyResolver(null));
            await using (LocalSequentialFile file = new(path))
                Assert.Equal(
                    rows.Length,
                    await format.WriteAsync(file, schema, input.Reader, cancellationToken));

            Channel<RecordBatch> output = Channel.CreateUnbounded<RecordBatch>();
            await using (LocalRandomAccessFile file = new(path))
                await format.ReadAsync(file, schema, output.Writer, cancellationToken: cancellationToken);
            output.Writer.Complete();

            using RecordBatch batch = await output.Reader.ReadAsync(cancellationToken);
            Assert.Equivalent(rows, NestedListRow.ListFromRecordBatch(batch), strict: true);
            Assert.False(output.Reader.TryRead(out _));
        }
        finally
        {
            File.Delete(path);
        }
    }
}

[ArrowSerializable]
public partial record NestedListRow
{
    public required List<int> Values { get; init; }
    public required List<List<int>> NestedValues { get; init; }
}
