using System.Numerics;
using EngineeredWood.Expressions;
using Iceberg.Net.Schemas;
using Iceberg.Net.Serialization;

namespace Iceberg.Net.Tests;

public class IcebergLiteralSerializerTests
{
    [Fact]
    public void SerializesAppendixDPrimitiveRepresentations()
    {
        Assert.Equal([1], Serialize(new PrimitiveType.Boolean(), LiteralValue.Of(true)));
        Assert.Equal([0x78, 0x56, 0x34, 0x12], Serialize(new PrimitiveType.Int(), LiteralValue.Of(0x12345678)));
        Assert.Equal(
            [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01],
            Serialize(new PrimitiveType.Long(), LiteralValue.Of(0x0102030405060708L)));
        Assert.Equal("iceberg"u8.ToArray(), Serialize(new PrimitiveType.String(), LiteralValue.Of("iceberg")));
        Assert.Equal(
            Convert.FromHexString("F79C3E09677C4BBDA4793F349CB785E7"),
            Serialize(new PrimitiveType.Uuid(), LiteralValue.Of(Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7"))));
        Assert.Equal(
            [0x00, 0x01, 0x02, 0xff],
            Serialize(new PrimitiveType.Fixed(4), LiteralValue.Of([0, 1, 2, 255])));
        Assert.Equal(
            [0x00, 0x80],
            Serialize(
                new PrimitiveType.Decimal(5, 2),
                LiteralValue.HighPrecisionDecimalOf(new BigInteger(128), 2)));
        Assert.Equal(
            [0xff, 0x7f],
            Serialize(
                new PrimitiveType.Decimal(5, 2),
                LiteralValue.HighPrecisionDecimalOf(new BigInteger(-129), 2)));
    }

    [Fact]
    public void RoundTripsLogicalPrimitiveValues()
    {
        DateOnly date = new(2017, 11, 16);
        TimeOnly time = new TimeOnly(22, 31, 8, 123).Add(TimeSpan.FromTicks(4560));
        DateTimeOffset timestamp = new(2017, 11, 16, 22, 31, 8, 123, TimeSpan.Zero);
        timestamp = timestamp.AddTicks(4560);

        Assert.Equal(date, RoundTrip(new PrimitiveType.Date(), LiteralValue.Of(date)).AsDateOnly);
        Assert.Equal(time, RoundTrip(new PrimitiveType.Time(), LiteralValue.Of(time)).AsTimeOnly);
        Assert.Equal(
            timestamp,
            RoundTrip(new PrimitiveType.Timestamp(), LiteralValue.Of(timestamp)).AsDateTimeOffset);
        Assert.Equal(
            timestamp,
            RoundTrip(new PrimitiveType.TimestampTz(), LiteralValue.Of(timestamp)).AsDateTimeOffset);
        Assert.Equal(
            checked((timestamp.Ticks - DateTimeOffset.UnixEpoch.Ticks) * 100),
            RoundTrip(new PrimitiveType.TimestampNs(), LiteralValue.Of(timestamp)).AsInt64);
        Assert.Equal(
            checked((timestamp.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) * 100),
            RoundTrip(new PrimitiveType.TimestampTzNs(), LiteralValue.Of(timestamp)).AsInt64);
    }

    [Fact]
    public void DeserializesDecimalWithDeclaredScale()
    {
        LiteralValue value = IcebergLiteralSerializer.Deserialize(
            new PrimitiveType.Decimal(38, 18),
            Convert.FromHexString("010203040506070809"));

        (BigInteger unscaled, int scale) = value.AsHighPrecisionDecimal;
        Assert.Equal(BigInteger.Parse("18591708106338011145"), unscaled);
        Assert.Equal(18, scale);
    }

    [Fact]
    public void RejectsUnsupportedValuesAndPreservesNanoseconds()
    {
        Assert.Throws<NotSupportedException>(() => IcebergLiteralSerializer.Serialize(
            new StructType([]),
            LiteralValue.Of(1)));
        Assert.Equal(
            1,
            IcebergLiteralSerializer.Deserialize(
                    new PrimitiveType.TimestampNs(),
                    [1, 0, 0, 0, 0, 0, 0, 0])
                .AsInt64);
    }

    private static byte[] Serialize(IIcebergType type, LiteralValue value) =>
        IcebergLiteralSerializer.Serialize(type, value);

    private static LiteralValue RoundTrip(IIcebergType type, LiteralValue value) =>
        IcebergLiteralSerializer.Deserialize(type, IcebergLiteralSerializer.Serialize(type, value));
}