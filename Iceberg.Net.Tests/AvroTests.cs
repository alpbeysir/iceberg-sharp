using System.Text;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;

namespace Iceberg.Net.Tests;

public class AvroTests
{
    [Fact]
    public void PublicOcfApiPreservesSchemaMetadataAndRecords()
    {
        const string schemaJson =
            """{"type":"record","name":"manifest_entry","fields":[{"name":"value","type":"long","field-id":42}]}""";
        Dictionary<string, byte[]> metadata = new()
        {
            ["format-version"] = "2"u8.ToArray()
        };

        using MemoryStream stream = new();
        using (OcfWriter writer = new(stream, AvroCodec.Null))
        {
            writer.WriteHeader(new AvroSchema(schemaJson), metadata);

            AvroBinaryWriter binaryWriter = new();
            binaryWriter.WriteLong(123);
            writer.WriteBlock(binaryWriter.WrittenSpan, 1);
        }

        stream.Position = 0;
        using OcfReader reader = OcfReader.Open(stream);

        Assert.Equal(schemaJson, reader.WriterSchema.Json);
        Assert.Equal("2", Encoding.UTF8.GetString(reader.Metadata["format-version"]));

        (ReadOnlyMemory<byte> data, long objectCount) = Assert.IsType<
            (ReadOnlyMemory<byte> data, long objectCount)>(reader.ReadBlock());
        Assert.Equal(1, objectCount);

        AvroBinaryReader binaryReader = new(data.Span);
        Assert.Equal(123, binaryReader.ReadLong());
        Assert.True(binaryReader.IsEmpty);
        Assert.Null(reader.ReadBlock());
    }
}
