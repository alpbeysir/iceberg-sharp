using System.Text;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;
using System.Text.Json;
using Iceberg.Net.Schemas;
using ContainerAvroSchema = EngineeredWood.Avro.AvroSchema;

namespace Iceberg.Net.Tests;

public class AvroTests
{
    [Fact]
    public void IcebergSchemaConvertsToAvroWithIcebergAnnotations()
    {
        Schema iceberg = new(
        [
            new StructField(1, "event_time", new PrimitiveType.TimestampTz(), false),
            new StructField(2, "values", new ListType(3, new PrimitiveType.Long(), true), true),
            new StructField(
                4,
                "lookup",
                new MapType(5, new PrimitiveType.Int(), 6, new PrimitiveType.String(), false),
                true)
        ]);

        ContainerAvroSchema avro = AvroSchemas.FromSchema(iceberg, "manifest");
        using JsonDocument document = JsonDocument.Parse(avro.Json);
        JsonElement fields = document.RootElement.GetProperty("fields");
        JsonElement timestamp = fields[0].GetProperty("type")[1];
        JsonElement list = fields[1].GetProperty("type");
        JsonElement map = fields[2].GetProperty("type");

        Assert.Equal(1, fields[0].GetProperty("field-id").GetInt32());
        Assert.Equal(JsonValueKind.Null, fields[0].GetProperty("default").ValueKind);
        Assert.Equal("timestamp-micros", timestamp.GetProperty("logicalType").GetString());
        Assert.True(timestamp.GetProperty("adjust-to-utc").GetBoolean());
        Assert.Equal(3, list.GetProperty("element-id").GetInt32());
        Assert.Equal("map", map.GetProperty("logicalType").GetString());
        Assert.Equal(5, map.GetProperty("items").GetProperty("fields")[0]
            .GetProperty("field-id")
            .GetInt32());
        Assert.Equal(JsonValueKind.Null, map.GetProperty("items").GetProperty("fields")[1]
            .GetProperty("default")
            .ValueKind);
        AvroRecordSchema reparsed = Assert.IsType<AvroRecordSchema>(new ContainerAvroSchema(avro.Json).Root);
        Assert.Equal("map", Assert.IsType<AvroArraySchema>(reparsed.Fields[2].Schema).LogicalType);
    }

    [Fact]
    public void IcebergSchemaUsesReferenceCompatibleNamesAndNativeStringMaps()
    {
        Schema iceberg = new(
        [
            new StructField(1, "9 bad.name\"", new PrimitiveType.Uuid(), true),
            new StructField(2, "fixed", new PrimitiveType.Fixed(4), true),
            new StructField(3, "amount", new PrimitiveType.Decimal(9, 2), true),
            new StructField(
                4,
                "lookup",
                new MapType(5, new PrimitiveType.String(), 6, new PrimitiveType.Long(), false),
                true)
        ]);

        using JsonDocument document = JsonDocument.Parse(AvroSchemas.FromSchema(iceberg, "table").Json);
        JsonElement fields = document.RootElement.GetProperty("fields");

        Assert.Equal("_9_x20bad_x2Ename_x22", fields[0].GetProperty("name").GetString());
        Assert.Equal("9 bad.name\"", fields[0].GetProperty("iceberg-field-name").GetString());
        Assert.Equal("uuid_fixed", fields[0].GetProperty("type").GetProperty("name").GetString());
        Assert.Equal("fixed_4", fields[1].GetProperty("type").GetProperty("name").GetString());
        Assert.Equal("decimal_9_2", fields[2].GetProperty("type").GetProperty("name").GetString());
        Assert.Equal(4, fields[2].GetProperty("type").GetProperty("size").GetInt32());

        JsonElement map = fields[3].GetProperty("type");
        Assert.Equal("map", map.GetProperty("type").GetString());
        Assert.Equal(5, map.GetProperty("key-id").GetInt32());
        Assert.Equal(6, map.GetProperty("value-id").GetInt32());
        Assert.Equal(JsonValueKind.Array, map.GetProperty("values").ValueKind);
    }

    [Fact]
    public void TypedSchemaApiWritesCustomProperties()
    {
        AvroFieldNode value = new(
            "event_time",
            new AvroUnionSchema(
            [
                AvroPrimitiveSchema.Null,
                new AvroPrimitiveSchema(AvroType.Long)
                {
                    LogicalType = "timestamp-micros",
                    CustomProperties = new Dictionary<string, AvroValue>
                    {
                        ["adjust-to-utc"] = true
                    }
                }
            ]))
        {
            Default = AvroValue.Null,
            CustomProperties = new Dictionary<string, AvroValue>
            {
                ["field-id"] = 1000
            }
        };
        AvroFieldNode partitionField = new(
            "partition",
            new AvroRecordSchema("r102", null, [value]))
        {
            CustomProperties = new Dictionary<string, AvroValue>
            {
                ["field-id"] = 102
            }
        };
        ContainerAvroSchema schema = new(new AvroRecordSchema("root", null, [partitionField])
        {
            CustomProperties = new Dictionary<string, AvroValue>
            {
                ["iceberg"] = "yes"
            }
        });

        using JsonDocument document = JsonDocument.Parse(schema.Json);
        JsonElement root = document.RootElement;
        JsonElement partition = root.GetProperty("fields")[0];
        JsonElement eventTime = partition.GetProperty("type").GetProperty("fields")[0];
        JsonElement timestamp = eventTime.GetProperty("type")[1];

        Assert.Equal("yes", root.GetProperty("iceberg").GetString());
        Assert.Equal(102, partition.GetProperty("field-id").GetInt32());
        Assert.Equal(1000, eventTime.GetProperty("field-id").GetInt32());
        Assert.Equal(JsonValueKind.Null, eventTime.GetProperty("default").ValueKind);
        Assert.True(timestamp.GetProperty("adjust-to-utc").GetBoolean());

        AvroRecordSchema parsedRoot = Assert.IsType<AvroRecordSchema>(new ContainerAvroSchema(schema.Json).Root);
        Assert.Equal("yes", parsedRoot.CustomProperties["iceberg"].AsString);
        AvroRecordSchema parsedPartition = Assert.IsType<AvroRecordSchema>(parsedRoot.Fields[0].Schema);
        AvroFieldNode parsedValue = parsedPartition.Fields[0];
        Assert.Equal(1000, parsedValue.CustomProperties["field-id"].AsInt32);
        Assert.Equal(AvroValueKind.Null, parsedValue.Default!.Value.Kind);
        AvroUnionSchema parsedUnion = Assert.IsType<AvroUnionSchema>(parsedValue.Schema);
        Assert.True(Assert.IsType<AvroPrimitiveSchema>(parsedUnion.Branches[1])
            .CustomProperties["adjust-to-utc"]
            .AsBoolean);
    }

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
            writer.WriteHeader(new ContainerAvroSchema(schemaJson), metadata);

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
