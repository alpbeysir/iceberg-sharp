using Apache.Arrow;
using Apache.Arrow.Types;

namespace Iceberg.Net.Schemas;

public static class ArrowSchemas
{
    public static Apache.Arrow.Schema FromSchema(Schema schema)
    {
        IEnumerable<Field> fields = schema.Fields.Select(field => new Field(
            field.Name,
            FromIcebergType(field.FieldType),
            !field.Required,
            FieldIdMetadata(field.Id)));
        return new Apache.Arrow.Schema(fields, []);
    }

    private static IEnumerable<KeyValuePair<string, string>> FieldIdMetadata(int fieldId)
    {
        yield return KeyValuePair.Create("PARQUET:field_id", fieldId.ToString());
    }

    public static IArrowType FromIcebergType(IIcebergType type)
    {
        return type switch
        {
            ListType listType => new Apache.Arrow.Types.ListType(
                new Field(
                    "element",
                    FromIcebergType(listType.Element),
                    !listType.ElementRequired,
                    FieldIdMetadata(listType.ElementId))),
            MapType mapType => new Apache.Arrow.Types.MapType(
                new Field("key", FromIcebergType(mapType.Key), false, FieldIdMetadata(mapType.KeyId)),
                new Field(
                    "value",
                    FromIcebergType(mapType.Value),
                    !mapType.ValueRequired,
                    FieldIdMetadata(mapType.ValueId))),
            StructType structType => FromStructType(structType),
            PrimitiveType primitiveType => FromPrimitive(primitiveType),

            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };
    }

    private static Apache.Arrow.Types.StructType FromStructType(StructType structType)
    {
        IEnumerable<Field> fields =
            structType.Fields.Select(field => new Field(
                field.Name,
                FromIcebergType(field.FieldType),
                !field.Required,
                FieldIdMetadata(field.Id)));
        return new Apache.Arrow.Types.StructType(fields.ToList());
    }

    private static IArrowType FromPrimitive(
        PrimitiveType primitiveType)
    {
        primitiveType = PrimitiveType.Parse(primitiveType.Name);
        return primitiveType switch
        {
            PrimitiveType.Binary => new BinaryType(),
            PrimitiveType.Boolean => new BooleanType(),
            PrimitiveType.Date => new Date32Type(),
            PrimitiveType.Decimal @decimal => FromDecimal(@decimal),
            PrimitiveType.Double => new DoubleType(),
            PrimitiveType.Fixed @fixed => new FixedSizeBinaryType(@fixed.L),
            PrimitiveType.Float => new FloatType(),
            PrimitiveType.Int => new Int32Type(),
            PrimitiveType.Long => new Int64Type(),
            PrimitiveType.String => new StringType(),
            PrimitiveType.Time => TimeType.Microsecond,
            PrimitiveType.Timestamp => new TimestampType(TimeUnit.Microsecond, (string)null!),
            PrimitiveType.TimestampNs => new TimestampType(TimeUnit.Nanosecond, (string)null!),
            PrimitiveType.TimestampTz => new TimestampType(TimeUnit.Microsecond, "UTC"),
            PrimitiveType.TimestampTzNs => new TimestampType(TimeUnit.Nanosecond, "UTC"),
            PrimitiveType.Uuid => new FixedSizeBinaryType(16),
            _ => throw new ArgumentOutOfRangeException(nameof(primitiveType))
        };
    }

    private static IArrowType FromDecimal(PrimitiveType.Decimal type)
    {
        return new Decimal128Type(type.P, type.S);
    }
}