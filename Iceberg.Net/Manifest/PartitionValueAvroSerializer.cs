using System.Buffers.Binary;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Expressions;
using Iceberg.Net.Schemas;
using Iceberg.Net.Serialization;

namespace Iceberg.Net.Metadata;

internal static class PartitionValueAvroSerializer
{
    internal static void Write(
        AvroBinaryWriter writer,
        PrimitiveType type,
        LiteralValue? value)
    {
        bool isNull = value is null || value.Value.IsNull;
        writer.WriteUnionIndex(isNull ? 0 : 1);
        if (isNull) return;

        LiteralValue literal = value!.Value;
        switch (literal.Type)
        {
            case LiteralValue.Kind.Boolean when type is PrimitiveType.Boolean:
                writer.WriteBoolean(literal.AsBoolean);
                break;
            case LiteralValue.Kind.Int32 when type is PrimitiveType.Int:
                writer.WriteInt(literal.AsInt32);
                break;
            case LiteralValue.Kind.Int64 when type is PrimitiveType.Long:
                writer.WriteLong(literal.AsInt64);
                break;
            case LiteralValue.Kind.Float when type is PrimitiveType.Float:
                writer.WriteFloat(literal.AsFloat);
                break;
            case LiteralValue.Kind.Double when type is PrimitiveType.Double:
                writer.WriteDouble(literal.AsDouble);
                break;
            case LiteralValue.Kind.String when type is PrimitiveType.String:
                writer.WriteString(literal.AsString);
                break;
            case LiteralValue.Kind.Binary when type is PrimitiveType.Binary:
                writer.WriteBytes(literal.AsBinary);
                break;
            case LiteralValue.Kind.Binary when type is PrimitiveType.Fixed fixedType:
                writer.WriteFixed(IcebergLiteralSerializer.FixedBytes(literal, fixedType.L));
                break;
            case LiteralValue.Kind.Guid when type is PrimitiveType.Uuid:
                writer.WriteFixed(IcebergLiteralSerializer.UuidBytes(literal.AsGuid));
                break;
            case LiteralValue.Kind.Decimal or LiteralValue.Kind.HighPrecisionDecimal
                when type is PrimitiveType.Decimal decimalType:
                writer.WriteFixed(IcebergLiteralSerializer.SignExtend(
                    IcebergLiteralSerializer.GetUnscaledDecimal(literal, decimalType.S),
                    IcebergLiteralSerializer.DecimalRequiredBytes(decimalType.P)));
                break;
            case LiteralValue.Kind.DateOnly when type is PrimitiveType.Date:
                writer.WriteInt(IcebergLiteralSerializer.ToDays(literal.AsDateOnly));
                break;
            case LiteralValue.Kind.TimeOnly when type is PrimitiveType.Time:
                writer.WriteLong(IcebergLiteralSerializer.ToMicroseconds(literal.AsTimeOnly));
                break;
            case LiteralValue.Kind.DateTimeOffset when type is PrimitiveType.Timestamp:
                writer.WriteLong(IcebergLiteralSerializer.ToTimestamp(
                    literal.AsDateTimeOffset,
                    adjustedToUtc: false,
                    nanoseconds: false));
                break;
            case LiteralValue.Kind.DateTimeOffset when type is PrimitiveType.TimestampTz:
                writer.WriteLong(IcebergLiteralSerializer.ToTimestamp(
                    literal.AsDateTimeOffset,
                    adjustedToUtc: true,
                    nanoseconds: false));
                break;
            case LiteralValue.Kind.DateTimeOffset or LiteralValue.Kind.Int64
                when type is PrimitiveType.TimestampNs:
                writer.WriteLong(IcebergLiteralSerializer.ToNanoseconds(literal, adjustedToUtc: false));
                break;
            case LiteralValue.Kind.DateTimeOffset or LiteralValue.Kind.Int64
                when type is PrimitiveType.TimestampTzNs:
                writer.WriteLong(IcebergLiteralSerializer.ToNanoseconds(literal, adjustedToUtc: true));
                break;
            default:
                throw new ArgumentException(
                    $"A {literal.Type} literal cannot be written as an Iceberg {type.Name} partition value.",
                    nameof(value));
        }
    }

    internal static LiteralValue? Read(
        ref AvroBinaryReader reader,
        PrimitiveType type)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        type = PrimitiveType.Parse(type.Name);
        return type switch
        {
            PrimitiveType.Boolean => LiteralValue.Of(reader.ReadBoolean()),
            PrimitiveType.Int => LiteralValue.Of(reader.ReadInt()),
            PrimitiveType.Long => LiteralValue.Of(reader.ReadLong()),
            PrimitiveType.Float => LiteralValue.Of(reader.ReadFloat()),
            PrimitiveType.Double => LiteralValue.Of(reader.ReadDouble()),
            PrimitiveType.String => LiteralValue.Of(reader.ReadString()),
            PrimitiveType.Binary => LiteralValue.Of(reader.ReadBytes().ToArray()),
            PrimitiveType.Date => DeserializeInt(ref reader, type),
            PrimitiveType.Time or PrimitiveType.Timestamp or PrimitiveType.TimestampTz or
                PrimitiveType.TimestampNs or PrimitiveType.TimestampTzNs => DeserializeLong(ref reader, type),
            PrimitiveType.Uuid => IcebergLiteralSerializer.Deserialize(type, reader.ReadFixed(16)),
            PrimitiveType.Fixed fixedType =>
                IcebergLiteralSerializer.Deserialize(type, reader.ReadFixed(fixedType.L)),
            PrimitiveType.Decimal decimalType => IcebergLiteralSerializer.Deserialize(
                type,
                reader.ReadFixed(IcebergLiteralSerializer.DecimalRequiredBytes(decimalType.P))),
            _ => throw new NotSupportedException(
                $"Iceberg type {type.Name} cannot be used as a partition value.")
        };
    }

    private static LiteralValue DeserializeInt(ref AvroBinaryReader reader, PrimitiveType type)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, reader.ReadInt());
        return IcebergLiteralSerializer.Deserialize(type, bytes);
    }

    private static LiteralValue DeserializeLong(ref AvroBinaryReader reader, PrimitiveType type)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, reader.ReadLong());
        return IcebergLiteralSerializer.Deserialize(type, bytes);
    }
}
