using System.Collections.Immutable;
using EngineeredWood.Avro.Encoding;

namespace Iceberg.Net.Metadata;

internal static class AvroSerializationUtilities
{
    internal static bool TryReadArrayBlock(ref AvroBinaryReader reader, out long count)
    {
        count = reader.ReadLong();
        if (count == 0) return false;
        if (count < 0)
        {
            count = -count;
            reader.ReadLong();
        }

        return true;
    }

    internal static void WriteArrayStart(AvroBinaryWriter writer, int count)
    {
        if (count > 0) writer.WriteLong(count);
    }

    internal static void WriteNullableLong(AvroBinaryWriter writer, long? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteLong(value.Value);
    }

    internal static long? ReadNullableLong(ref AvroBinaryReader reader, bool skip = false)
    {
        ThrowIfSimpleTypeSkip(skip);
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadLong();
    }

    internal static void WriteNullableInt(AvroBinaryWriter writer, int? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteInt(value.Value);
    }

    internal static int? ReadNullableInt(ref AvroBinaryReader reader, bool skip = false)
    {
        ThrowIfSimpleTypeSkip(skip);
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadInt();
    }

    internal static void WriteNullableBoolean(AvroBinaryWriter writer, bool? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteBoolean(value.Value);
    }

    internal static bool? ReadNullableBoolean(ref AvroBinaryReader reader, bool skip = false)
    {
        ThrowIfSimpleTypeSkip(skip);
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadBoolean();
    }

    internal static void WriteNullableBytes(AvroBinaryWriter writer, byte[]? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteBytes(value);
    }

    internal static byte[]? ReadNullableBytes(ref AvroBinaryReader reader, bool skip = false)
    {
        ThrowIfSimpleTypeSkip(skip);
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadBytes().ToArray();
    }

    internal static void WriteNullableString(AvroBinaryWriter writer, string? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteString(value);
    }

    internal static string? ReadNullableString(ref AvroBinaryReader reader, bool skip = false)
    {
        ThrowIfSimpleTypeSkip(skip);
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadString();
    }

    internal static void WriteNullableLongArray(
        AvroBinaryWriter writer,
        ImmutableArray<long>? values)
    {
        if (values is null)
        {
            writer.WriteUnionIndex(0);
            return;
        }

        writer.WriteUnionIndex(1);
        WriteArrayStart(writer, values.Value.Length);
        foreach (long value in values.Value) writer.WriteLong(value);
        writer.WriteLong(0);
    }

    internal static ImmutableArray<long>? ReadNullableLongArray(
        ref AvroBinaryReader reader,
        bool skip = false)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableArray<long>.Builder? values = skip ? null : ImmutableArray.CreateBuilder<long>();
        while (TryReadArrayBlock(ref reader, out long count))
            for (long i = 0; i < count; i++)
            {
                long value = reader.ReadLong();
                values?.Add(value);
            }

        return values?.ToImmutable();
    }

    internal static void WriteNullableIntArray(
        AvroBinaryWriter writer,
        ImmutableArray<int>? values)
    {
        if (values is null)
        {
            writer.WriteUnionIndex(0);
            return;
        }

        writer.WriteUnionIndex(1);
        WriteArrayStart(writer, values.Value.Length);
        foreach (int value in values.Value) writer.WriteInt(value);
        writer.WriteLong(0);
    }

    internal static ImmutableArray<int>? ReadNullableIntArray(
        ref AvroBinaryReader reader,
        bool skip = false)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableArray<int>.Builder? values = skip ? null : ImmutableArray.CreateBuilder<int>();
        while (TryReadArrayBlock(ref reader, out long count))
            for (long i = 0; i < count; i++)
            {
                int value = reader.ReadInt();
                values?.Add(value);
            }

        return values?.ToImmutable();
    }

    private static void ThrowIfSimpleTypeSkip(bool skip)
    {
        if (skip)
            throw new ArgumentException(
                "Skipping a simple Avro value is not supported because it must be decoded to advance the reader.",
                nameof(skip));
    }
}
