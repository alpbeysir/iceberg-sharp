using Apache.Arrow;
using Apache.Arrow.Types;

namespace Iceberg.Net.Schemas;

public class ArrowSchema
{
    public static Apache.Arrow.Schema FromSchema(Schema schema)
    {
        var fields = schema.Fields.Select(field => new Field(
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
        var fields =
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
            PrimitiveType.Timestamp => TimestampType.Default,
            PrimitiveType.TimestampNs => TimeType.Nanosecond,
            PrimitiveType.TimestampTz => TimestampType.Default,
            PrimitiveType.TimestampTzNs => TimestampType.Default,
            PrimitiveType.Uuid => new FixedSizeBinaryType(16),
            _ => throw new ArgumentOutOfRangeException(nameof(primitiveType))
        };
    }

    private static IArrowType FromDecimal(PrimitiveType.Decimal type)
    {
        return type.P switch
        {
            <= 9 => new Int32Type(),
            <= 18 => new Int64Type(),
            _ => new FixedSizeBinaryType(GetDecimalByteLength(type.P))
        };
    }

    private static int GetDecimalByteLength(int precision)
    {
        // The formula for the number of bytes required for a given precision:
        // bytes = ceil(log2(10^precision - 1) / 8)
        // For simplicity and speed, most implementations use this standard mapping:
        return precision switch
        {
            <= 9 => 4, // Fits in Int32
            <= 18 => 8, // Fits in Int64
            <= 22 => 10,
            <= 26 => 12,
            <= 30 => 14,
            <= 34 => 16,
            <= 38 => 20,
            _ => throw new ArgumentException("Precision cannot exceed 38", nameof(precision))
        };
    }
}