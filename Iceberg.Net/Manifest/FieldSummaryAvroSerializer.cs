using System.Collections.Immutable;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Expressions;
using Iceberg.Net.Schemas;
using Iceberg.Net.Serialization;

namespace Iceberg.Net.Metadata;

internal static class FieldSummaryAvroSerializer
{
    internal static void WriteNullableArray(
        AvroBinaryWriter writer,
        ImmutableArray<FieldSummary>? summaries,
        IReadOnlyList<PrimitiveType> partitionTypes)
    {
        if (summaries is null)
        {
            writer.WriteUnionIndex(0);
            return;
        }

        if (summaries.Value.Length != partitionTypes.Count)
            throw new InvalidDataException(
                $"Manifest contains {summaries.Value.Length} partition summaries, but its partition spec " +
                $"contains {partitionTypes.Count} fields.");

        writer.WriteUnionIndex(1);
        AvroSerializationUtilities.WriteArrayStart(writer, summaries.Value.Length);
        for (int i = 0; i < summaries.Value.Length; i++)
        {
            FieldSummary summary = summaries.Value[i];
            writer.WriteBoolean(summary.ContainsNull);
            AvroSerializationUtilities.WriteNullableBoolean(writer, summary.ContainsNan);
            WriteNullableLiteral(writer, summary.LowerBound, partitionTypes[i]);
            WriteNullableLiteral(writer, summary.UpperBound, partitionTypes[i]);
        }

        writer.WriteLong(0);
    }

    internal static ImmutableArray<FieldSummary>? ReadNullableArray(
        ref AvroBinaryReader reader,
        IReadOnlyList<PrimitiveType> partitionTypes,
        bool skip = false)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableArray<FieldSummary>.Builder? summaries = skip
            ? null
            : ImmutableArray.CreateBuilder<FieldSummary>();
        int fieldIndex = 0;
        while (AvroSerializationUtilities.TryReadArrayBlock(ref reader, out long count))
        {
            for (long i = 0; i < count; i++)
            {
                bool containsNull = reader.ReadBoolean();
                bool? containsNan = AvroSerializationUtilities.ReadNullableBoolean(ref reader);
                if (fieldIndex >= partitionTypes.Count)
                    throw new InvalidDataException(
                        "Manifest contains more partition summaries than its partition spec.");
                PrimitiveType type = partitionTypes[fieldIndex++];
                LiteralValue? lowerBound = ReadNullableLiteral(ref reader, type, skip);
                LiteralValue? upperBound = ReadNullableLiteral(ref reader, type, skip);
                summaries?.Add(new FieldSummary(
                    containsNan,
                    containsNull,
                    lowerBound,
                    upperBound));
            }
        }

        if (fieldIndex != partitionTypes.Count)
            throw new InvalidDataException(
                $"Manifest contains {fieldIndex} partition summaries, but its partition spec contains " +
                $"{partitionTypes.Count} fields.");

        return summaries?.ToImmutable();
    }

    private static void WriteNullableLiteral(
        AvroBinaryWriter writer,
        LiteralValue? value,
        IIcebergType type)
    {
        bool isNull = value is null || value.Value.IsNull;
        writer.WriteUnionIndex(isNull ? 0 : 1);
        if (!isNull) writer.WriteBytes(IcebergLiteralSerializer.Serialize(type, value!.Value));
    }

    private static LiteralValue? ReadNullableLiteral(
        ref AvroBinaryReader reader,
        IIcebergType type,
        bool skip)
    {
        if (reader.ReadUnionIndex() == 0) return null;
        ReadOnlySpan<byte> bytes = reader.ReadBytes();
        if (skip) return null;
        return IcebergLiteralSerializer.Deserialize(type, bytes);
    }
}
