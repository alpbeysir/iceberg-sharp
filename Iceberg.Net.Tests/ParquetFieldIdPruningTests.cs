using System.Threading.Channels;
using Apache.Arrow;
using Iceberg.Net.Catalog;
using Iceberg.Net.Parquet;
using Iceberg.Net.Schemas;
using IcebergSchema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Tests;

public class ParquetFieldIdPruningTests
{
    [Fact]
    public async Task ReadAsyncProjectsColumnsByFieldId()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
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

        ParquetDataFileFormat format = new(new TablePropertyResolver(null));
        using MemoryStream stream = new();
        Assert.Equal(
            2,
            await format.WriteAsync(stream, schema, input.Reader, cancellationToken));

        stream.Position = 0;
        Channel<RecordBatch> output = Channel.CreateUnbounded<RecordBatch>();
        await format.ReadAsync(
            stream,
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
    }
}
