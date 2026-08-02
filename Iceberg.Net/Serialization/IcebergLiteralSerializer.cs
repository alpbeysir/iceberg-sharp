using System.Buffers.Binary;
using System.Numerics;
using System.Text;
using EngineeredWood.Expressions;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Serialization;

/// <summary>
/// Serializes primitive Iceberg values using Appendix D's binary single-value format.
/// </summary>
/// <remarks>
/// Nanosecond timestamps deserialize as an <see cref="LiteralValue.Kind.Int64"/> containing the
/// exact epoch-nanosecond value because <see cref="DateTimeOffset"/> only has 100-nanosecond precision.
/// </remarks>
public static class IcebergLiteralSerializer
{
    private static readonly DateOnly EpochDate = new(1970, 1, 1);
    private static readonly DateTimeOffset EpochTimestamp = DateTimeOffset.UnixEpoch;

    public static byte[] Serialize(IIcebergType type, LiteralValue value)
    {
        PrimitiveType primitive = Normalize(type);

        return primitive switch
        {
            PrimitiveType.Boolean => [value.AsBoolean ? (byte)1 : (byte)0],
            PrimitiveType.Int => LittleEndian(value.AsInt32),
            PrimitiveType.Long => LittleEndian(value.AsInt64),
            PrimitiveType.Float => LittleEndian(value.AsFloat),
            PrimitiveType.Double => LittleEndian(value.AsDouble),
            PrimitiveType.Date => LittleEndian(ToDays(value.AsDateOnly)),
            PrimitiveType.Time => LittleEndian(ToMicroseconds(value.AsTimeOnly)),
            PrimitiveType.Timestamp => LittleEndian(ToTimestamp(value.AsDateTimeOffset, false, false)),
            PrimitiveType.TimestampTz => LittleEndian(ToTimestamp(value.AsDateTimeOffset, true, false)),
            PrimitiveType.TimestampNs => LittleEndian(ToNanoseconds(value, false)),
            PrimitiveType.TimestampTzNs => LittleEndian(ToNanoseconds(value, true)),
            PrimitiveType.String => Encoding.UTF8.GetBytes(value.AsString),
            PrimitiveType.Uuid => UuidBytes(value.AsGuid),
            PrimitiveType.Fixed fixedType => FixedBytes(value, fixedType.L),
            PrimitiveType.Binary => [.. value.AsBinary],
            PrimitiveType.Decimal decimalType => DecimalBytes(value, decimalType.S),
            _ => throw Unsupported(type)
        };
    }

    public static LiteralValue Deserialize(IIcebergType type, ReadOnlySpan<byte> bytes)
    {
        PrimitiveType primitive = Normalize(type);

        return primitive switch
        {
            PrimitiveType.Boolean => LiteralValue.Of(ReadBoolean(bytes)),
            PrimitiveType.Int => LiteralValue.Of(ReadInt32(bytes, primitive)),
            PrimitiveType.Long => LiteralValue.Of(ReadInt64(bytes, primitive)),
            PrimitiveType.Float => LiteralValue.Of(ReadSingle(bytes, primitive)),
            PrimitiveType.Double => LiteralValue.Of(ReadDouble(bytes, primitive)),
            PrimitiveType.Date => LiteralValue.Of(EpochDate.AddDays(ReadInt32(bytes, primitive))),
            PrimitiveType.Time => LiteralValue.Of(new TimeOnly(checked(ReadInt64(bytes, primitive) * 10))),
            PrimitiveType.Timestamp => LiteralValue.Of(ReadTimestamp(bytes, false, false, primitive)),
            PrimitiveType.TimestampTz => LiteralValue.Of(ReadTimestamp(bytes, true, false, primitive)),
            PrimitiveType.TimestampNs => LiteralValue.Of(ReadInt64(bytes, primitive)),
            PrimitiveType.TimestampTzNs => LiteralValue.Of(ReadInt64(bytes, primitive)),
            PrimitiveType.String => LiteralValue.Of(Encoding.UTF8.GetString(bytes)),
            PrimitiveType.Uuid => LiteralValue.Of(ReadUuid(bytes)),
            PrimitiveType.Fixed fixedType => LiteralValue.Of(ReadFixed(bytes, fixedType.L)),
            PrimitiveType.Binary => LiteralValue.Of(bytes.ToArray()),
            PrimitiveType.Decimal decimalType => LiteralValue.HighPrecisionDecimalOf(
                new BigInteger(bytes, isUnsigned: false, isBigEndian: true),
                decimalType.S),
            _ => throw Unsupported(type)
        };
    }

    internal static BigInteger GetUnscaledDecimal(LiteralValue value, int targetScale)
    {
        (BigInteger unscaled, int scale) = value.Type switch
        {
            LiteralValue.Kind.HighPrecisionDecimal => value.AsHighPrecisionDecimal,
            LiteralValue.Kind.Decimal => DecimalParts(value.AsDecimal),
            _ => throw new ArgumentException(
                $"Expected a decimal literal, but received {value.Type}.",
                nameof(value))
        };

        if (scale == targetScale) return unscaled;
        if (scale < targetScale) return unscaled * BigInteger.Pow(10, targetScale - scale);

        BigInteger divisor = BigInteger.Pow(10, scale - targetScale);
        BigInteger quotient = BigInteger.DivRem(unscaled, divisor, out BigInteger remainder);
        return remainder.IsZero
            ? quotient
            : throw new ArgumentException(
                $"Decimal literal with scale {scale} cannot be represented at scale {targetScale}.",
                nameof(value));
    }

    internal static int DecimalRequiredBytes(int precision)
    {
        if (precision <= 0) throw new ArgumentOutOfRangeException(nameof(precision));

        BigInteger maximum = BigInteger.Pow(10, precision) - 1;
        int bytes = 1;
        while (maximum > (BigInteger.One << (bytes * 8 - 1)) - 1) bytes++;
        return bytes;
    }

    internal static byte[] SignExtend(BigInteger value, int width)
    {
        byte[] minimal = value.ToByteArray(isUnsigned: false, isBigEndian: true);
        if (minimal.Length > width)
            throw new ArgumentOutOfRangeException(nameof(value), $"The decimal value does not fit in {width} bytes.");
        if (minimal.Length == width) return minimal;

        byte[] result = new byte[width];
        if (value.Sign < 0) Array.Fill(result, (byte)0xff);
        minimal.CopyTo(result, width - minimal.Length);
        return result;
    }

    private static PrimitiveType Normalize(IIcebergType type)
    {
        if (type is not PrimitiveType primitive) throw Unsupported(type);
        return PrimitiveType.Parse(primitive.Name);
    }

    private static byte[] DecimalBytes(LiteralValue value, int scale) =>
        GetUnscaledDecimal(value, scale).ToByteArray(isUnsigned: false, isBigEndian: true);

    private static (BigInteger Unscaled, int Scale) DecimalParts(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        BigInteger unscaled = (uint)bits[0]
                              | (new BigInteger((uint)bits[1]) << 32)
                              | (new BigInteger((uint)bits[2]) << 64);
        if ((bits[3] & int.MinValue) != 0) unscaled = -unscaled;
        return (unscaled, (bits[3] >> 16) & 0x7f);
    }

    internal static int ToDays(DateOnly value) => value.DayNumber - EpochDate.DayNumber;

    internal static long ToMicroseconds(TimeOnly value)
    {
        if (value.Ticks % 10 != 0)
            throw new ArgumentException("Iceberg time values must have microsecond precision.", nameof(value));
        return value.Ticks / 10;
    }

    internal static long ToTimestamp(DateTimeOffset value, bool adjustedToUtc, bool nanoseconds)
    {
        long ticks = adjustedToUtc
            ? value.UtcTicks - EpochTimestamp.UtcTicks
            : value.Ticks - EpochTimestamp.Ticks;

        if (!nanoseconds && ticks % 10 != 0)
            throw new ArgumentException("Iceberg timestamp values must have microsecond precision.", nameof(value));
        return nanoseconds ? checked(ticks * 100) : ticks / 10;
    }

    internal static long ToNanoseconds(LiteralValue value, bool adjustedToUtc) =>
        value.Type == LiteralValue.Kind.Int64
            ? value.AsInt64
            : ToTimestamp(value.AsDateTimeOffset, adjustedToUtc, true);

    private static DateTimeOffset ReadTimestamp(
        ReadOnlySpan<byte> bytes,
        bool adjustedToUtc,
        bool nanoseconds,
        PrimitiveType type)
    {
        long serialized = ReadInt64(bytes, type);
        long ticks = nanoseconds ? serialized / 100 : checked(serialized * 10);
        DateTimeOffset value = new(checked(EpochTimestamp.Ticks + ticks), TimeSpan.Zero);
        return adjustedToUtc ? value.ToUniversalTime() : value;
    }

    internal static byte[] UuidBytes(Guid value)
    {
        byte[] result = new byte[16];
        value.TryWriteBytes(result, bigEndian: true, out int bytesWritten);
        if (bytesWritten != result.Length) throw new InvalidOperationException("Could not serialize UUID.");
        return result;
    }

    private static Guid ReadUuid(ReadOnlySpan<byte> bytes)
    {
        RequireLength(bytes, 16, new PrimitiveType.Uuid());
        return new Guid(bytes, bigEndian: true);
    }

    internal static byte[] FixedBytes(LiteralValue value, int length)
    {
        byte[] bytes = value.AsBinary;
        RequireLength(bytes, length, new PrimitiveType.Fixed(length));
        return [.. bytes];
    }

    private static byte[] ReadFixed(ReadOnlySpan<byte> bytes, int length)
    {
        RequireLength(bytes, length, new PrimitiveType.Fixed(length));
        return bytes.ToArray();
    }

    private static bool ReadBoolean(ReadOnlySpan<byte> bytes)
    {
        RequireLength(bytes, 1, new PrimitiveType.Boolean());
        return bytes[0] != 0;
    }

    private static int ReadInt32(ReadOnlySpan<byte> bytes, PrimitiveType type)
    {
        RequireLength(bytes, sizeof(int), type);
        return BinaryPrimitives.ReadInt32LittleEndian(bytes);
    }

    private static long ReadInt64(ReadOnlySpan<byte> bytes, PrimitiveType type)
    {
        RequireLength(bytes, sizeof(long), type);
        return BinaryPrimitives.ReadInt64LittleEndian(bytes);
    }

    private static float ReadSingle(ReadOnlySpan<byte> bytes, PrimitiveType type)
    {
        RequireLength(bytes, sizeof(float), type);
        return BinaryPrimitives.ReadSingleLittleEndian(bytes);
    }

    private static double ReadDouble(ReadOnlySpan<byte> bytes, PrimitiveType type)
    {
        RequireLength(bytes, sizeof(double), type);
        return BinaryPrimitives.ReadDoubleLittleEndian(bytes);
    }

    private static byte[] LittleEndian(int value)
    {
        byte[] result = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(result, value);
        return result;
    }

    private static byte[] LittleEndian(long value)
    {
        byte[] result = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(result, value);
        return result;
    }

    private static byte[] LittleEndian(float value)
    {
        byte[] result = new byte[sizeof(float)];
        BinaryPrimitives.WriteSingleLittleEndian(result, value);
        return result;
    }

    private static byte[] LittleEndian(double value)
    {
        byte[] result = new byte[sizeof(double)];
        BinaryPrimitives.WriteDoubleLittleEndian(result, value);
        return result;
    }

    private static void RequireLength(ReadOnlySpan<byte> bytes, int expected, PrimitiveType type)
    {
        if (bytes.Length != expected)
            throw new InvalidDataException(
                $"An Iceberg {type.Name} value must contain {expected} bytes, but contained {bytes.Length}.");
    }

    private static NotSupportedException Unsupported(IIcebergType type) =>
        new($"Iceberg type {type} is not supported by binary single-value serialization.");
}
