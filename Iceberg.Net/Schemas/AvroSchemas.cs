using System.Globalization;
using System.Text;
using EngineeredWood.Avro.Schema;
using Iceberg.Net.Serialization;

namespace Iceberg.Net.Schemas;

/// <summary>Converts Iceberg schemas and types to their Iceberg Avro representation.</summary>
public static class AvroSchemas
{
    public static EngineeredWood.Avro.AvroSchema FromSchema(
        Schema schema,
        string recordName) =>
        new(FromStruct(schema, AvroName(recordName)));

    public static AvroSchemaNode FromIcebergType(IIcebergType type, int fieldId)
    {
        return type switch
        {
            StructType structType => FromStruct(structType, $"r{fieldId}"),
            ListType listType => FromList(listType),
            MapType mapType => FromMap(mapType),
            PrimitiveType primitiveType => FromPrimitive(primitiveType),
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };
    }

    private static AvroRecordSchema FromStruct(StructType type, string recordName) =>
        new(
            recordName,
            null,
            type.Fields.Select(FromField).ToArray());

    private static AvroFieldNode FromField(StructField field)
    {
        AvroSchemaNode type = FromIcebergType(field.FieldType, field.Id);
        if (!field.Required)
            type = new AvroUnionSchema([AvroPrimitiveSchema.Null, type]);

        bool validName = IsValidAvroName(field.Name);
        Dictionary<string, AvroValue> properties = new()
        {
            ["field-id"] = field.Id
        };
        if (!validName) properties["iceberg-field-name"] = field.Name;

        return new AvroFieldNode(validName ? field.Name : SanitizeAvroName(field.Name), type)
        {
            CustomProperties = properties,
            Default = field.Required ? (AvroValue?)null : AvroValue.Null,
            Doc = field.Doc
        };
    }

    private static AvroArraySchema FromList(ListType type) =>
        new(RequiredOrOptional(
            FromIcebergType(type.Element, type.ElementId),
            type.ElementRequired))
        {
            CustomProperties = new Dictionary<string, AvroValue>
            {
                ["element-id"] = type.ElementId
            }
        };

    private static AvroSchemaNode FromMap(MapType type)
    {
        if (type.Key is PrimitiveType keyType &&
            PrimitiveType.Parse(keyType.Name) is PrimitiveType.String)
        {
            return new AvroMapSchema(RequiredOrOptional(
                FromIcebergType(type.Value, type.ValueId),
                type.ValueRequired))
            {
                CustomProperties = new Dictionary<string, AvroValue>
                {
                    ["key-id"] = type.KeyId,
                    ["value-id"] = type.ValueId
                }
            };
        }

        AvroRecordSchema entry = new(
            $"k{type.KeyId}_v{type.ValueId}",
            null,
            [
                new AvroFieldNode("key", FromIcebergType(type.Key, type.KeyId))
                {
                    CustomProperties = FieldId(type.KeyId)
                },
                new AvroFieldNode(
                    "value",
                    RequiredOrOptional(
                        FromIcebergType(type.Value, type.ValueId),
                        type.ValueRequired))
                {
                    CustomProperties = FieldId(type.ValueId),
                    Default = type.ValueRequired ? (AvroValue?)null : AvroValue.Null
                }
            ]);

        return new AvroArraySchema(entry)
        {
            LogicalType = "map"
        };
    }

    private static AvroSchemaNode RequiredOrOptional(AvroSchemaNode type, bool required) =>
        required ? type : new AvroUnionSchema([AvroPrimitiveSchema.Null, type]);

    private static AvroSchemaNode FromPrimitive(PrimitiveType type)
    {
        type = PrimitiveType.Parse(type.Name);
        return type switch
        {
            PrimitiveType.Boolean => AvroPrimitiveSchema.Boolean,
            PrimitiveType.Int => AvroPrimitiveSchema.Int,
            PrimitiveType.Long => AvroPrimitiveSchema.Long,
            PrimitiveType.Float => AvroPrimitiveSchema.Float,
            PrimitiveType.Double => AvroPrimitiveSchema.Double,
            PrimitiveType.String => AvroPrimitiveSchema.String,
            PrimitiveType.Binary => AvroPrimitiveSchema.Bytes,
            PrimitiveType.Date => Logical(AvroType.Int, "date"),
            PrimitiveType.Time => Logical(AvroType.Long, "time-micros"),
            PrimitiveType.Timestamp => Timestamp("timestamp-micros", false),
            PrimitiveType.TimestampTz => Timestamp("timestamp-micros", true),
            PrimitiveType.TimestampNs => Timestamp("timestamp-nanos", false),
            PrimitiveType.TimestampTzNs => Timestamp("timestamp-nanos", true),
            PrimitiveType.Uuid => Fixed("uuid_fixed", 16, "uuid"),
            PrimitiveType.Fixed fixedType => Fixed($"fixed_{fixedType.L}", fixedType.L),
            PrimitiveType.Decimal decimalType => Decimal(decimalType),
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };
    }

    private static AvroPrimitiveSchema Logical(AvroType type, string logicalType) =>
        new(type) { LogicalType = logicalType };

    private static AvroPrimitiveSchema Timestamp(string logicalType, bool adjustedToUtc) =>
        new(AvroType.Long)
        {
            LogicalType = logicalType,
            CustomProperties = new Dictionary<string, AvroValue>
            {
                ["adjust-to-utc"] = adjustedToUtc
            }
        };

    private static AvroFixedSchema Fixed(string name, int size, string? logicalType = null) =>
        new(name, null, size) { LogicalType = logicalType };

    private static AvroFixedSchema Decimal(PrimitiveType.Decimal type) =>
        new($"decimal_{type.P}_{type.S}", null, IcebergLiteralSerializer.DecimalRequiredBytes(type.P))
        {
            LogicalType = "decimal",
            Precision = type.P,
            Scale = type.S
        };

    private static IReadOnlyDictionary<string, AvroValue> FieldId(int fieldId) =>
        new Dictionary<string, AvroValue> { ["field-id"] = fieldId };

    private static string AvroName(string name) =>
        IsValidAvroName(name) ? name : SanitizeAvroName(name);

    private static bool IsValidAvroName(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (!(char.IsLetter(name[0]) || name[0] == '_')) return false;
        for (int i = 1; i < name.Length; i++)
            if (!(char.IsLetterOrDigit(name[i]) || name[i] == '_'))
                return false;
        return true;
    }

    private static string SanitizeAvroName(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        StringBuilder result = new(name.Length);
        AppendCompatibleCharacter(result, name[0], first: true);
        for (int i = 1; i < name.Length; i++)
            AppendCompatibleCharacter(result, name[i], first: false);
        return result.ToString();
    }

    private static void AppendCompatibleCharacter(StringBuilder result, char value, bool first)
    {
        if ((char.IsLetter(value) || value == '_') || (!first && char.IsDigit(value)))
        {
            result.Append(value);
        }
        else if (first && char.IsDigit(value))
        {
            result.Append('_').Append(value);
        }
        else
        {
            result.Append("_x").Append(((int)value).ToString("X", CultureInfo.InvariantCulture));
        }
    }
}