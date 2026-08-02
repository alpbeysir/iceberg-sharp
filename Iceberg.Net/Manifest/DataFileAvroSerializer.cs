using System.Collections.Immutable;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Expressions;
using Iceberg.Net.Schemas;
using Iceberg.Net.Serialization;

namespace Iceberg.Net.Metadata;

internal static class DataFileAvroSerializer
{
    internal static void Write(
        AvroBinaryWriter writer,
        DataFile file,
        ManifestEntryTypes types)
    {
        IReadOnlyList<PrimitiveType> partitionTypes = types.PartitionTypes;
        if (file.Partition.IsDefault && partitionTypes.Count == 0)
            file = file with { Partition = [] };
        if (file.Partition.Length != partitionTypes.Count)
            throw new InvalidDataException(
                $"Data file has {file.Partition.Length} partition values, but partition spec " +
                $"{types.PartitionSpecId} has {partitionTypes.Count} fields.");

        writer.WriteInt((int)file.Content);
        writer.WriteString(file.FilePath);
        writer.WriteString(file.FileFormat);
        for (int i = 0; i < partitionTypes.Count; i++)
            PartitionValueAvroSerializer.Write(writer, partitionTypes[i], file.Partition[i]);
        writer.WriteLong(file.RecordCount);
        writer.WriteLong(file.FileSizeInBytes);
        WriteNullableLongMap(writer, file.ColumnSizes);
        WriteNullableLongMap(writer, file.ValueCounts);
        WriteNullableLongMap(writer, file.NullValueCounts);
        WriteNullableLongMap(writer, file.NanValueCounts);
        WriteNullableLiteralMap(writer, file.LowerBounds, types.FieldTypes);
        WriteNullableLiteralMap(writer, file.UpperBounds, types.FieldTypes);
        AvroSerializationUtilities.WriteNullableBytes(writer, file.KeyMetadata);
        AvroSerializationUtilities.WriteNullableLongArray(writer, file.SplitOffsets);
        AvroSerializationUtilities.WriteNullableIntArray(writer, file.EqualityIds);
        AvroSerializationUtilities.WriteNullableInt(writer, file.SortOrderId);
        AvroSerializationUtilities.WriteNullableString(writer, file.ReferencedDataFile);
    }

    internal static DataFile Read(
        ref AvroBinaryReader reader,
        ManifestEntryTypes types,
        IReadOnlySet<int>? fieldIds = null)
    {
        IReadOnlyList<PrimitiveType> partitionTypes = types.PartitionTypes;

        DataFileContent content = (DataFileContent)reader.ReadInt();
        string filePath = reader.ReadString();
        string fileFormat = reader.ReadString();
        bool skipPartition = ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.Partition);
        ImmutableArray<LiteralValue?>.Builder? partition = skipPartition
            ? null
            : ImmutableArray.CreateBuilder<LiteralValue?>(partitionTypes.Count);
        foreach (PrimitiveType type in partitionTypes)
        {
            LiteralValue? value = PartitionValueAvroSerializer.Read(ref reader, type);
            partition?.Add(value);
        }

        return new DataFile
        {
            Content = content,
            FilePath = filePath,
            FileFormat = fileFormat,
            Partition = partition?.MoveToImmutable() ?? [],
            RecordCount = reader.ReadLong(),
            FileSizeInBytes = reader.ReadLong(),
            ColumnSizes = ReadNullableLongMap(
                ref reader,
                ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.ColumnSizes)),
            ValueCounts = ReadNullableLongMap(
                ref reader,
                ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.ValueCounts)),
            NullValueCounts = ReadNullableLongMap(
                ref reader,
                ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.NullValueCounts)),
            NanValueCounts = ReadNullableLongMap(
                ref reader,
                ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.NanValueCounts)),
            LowerBounds = ReadNullableLiteralMap(
                ref reader,
                types.FieldTypes,
                ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.LowerBounds)),
            UpperBounds = ReadNullableLiteralMap(
                ref reader,
                types.FieldTypes,
                ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.UpperBounds)),
            KeyMetadata = AvroSerializationUtilities.ReadNullableBytes(ref reader),
            SplitOffsets = AvroSerializationUtilities.ReadNullableLongArray(
                ref reader,
                ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.SplitOffsets)),
            EqualityIds = AvroSerializationUtilities.ReadNullableIntArray(
                ref reader,
                ShouldSkip(fieldIds, ManifestSchemas.FieldIds.DataFile.EqualityIds)),
            SortOrderId = AvroSerializationUtilities.ReadNullableInt(ref reader),
            ReferencedDataFile = AvroSerializationUtilities.ReadNullableString(ref reader)
        };
    }

    private static void WriteNullableLongMap(
        AvroBinaryWriter writer,
        IReadOnlyDictionary<int, long>? values)
    {
        if (values is null)
        {
            writer.WriteUnionIndex(0);
            return;
        }

        writer.WriteUnionIndex(1);
        AvroSerializationUtilities.WriteArrayStart(writer, values.Count);
        foreach ((int key, long value) in values)
        {
            writer.WriteInt(key);
            writer.WriteLong(value);
        }

        writer.WriteLong(0);
    }

    private static ImmutableDictionary<int, long>? ReadNullableLongMap(
        ref AvroBinaryReader reader,
        bool skip)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableDictionary<int, long>.Builder? values = skip
            ? null
            : ImmutableDictionary.CreateBuilder<int, long>();
        while (AvroSerializationUtilities.TryReadArrayBlock(ref reader, out long count))
            for (long i = 0; i < count; i++)
            {
                int key = reader.ReadInt();
                long value = reader.ReadLong();
                values?.Add(key, value);
            }
        return values?.ToImmutable();
    }

    private static void WriteNullableLiteralMap(
        AvroBinaryWriter writer,
        IReadOnlyDictionary<int, LiteralValue>? values,
        IReadOnlyDictionary<int, IIcebergType> fieldTypes)
    {
        if (values is null)
        {
            writer.WriteUnionIndex(0);
            return;
        }

        writer.WriteUnionIndex(1);
        AvroSerializationUtilities.WriteArrayStart(writer, values.Count);
        foreach ((int key, LiteralValue value) in values)
        {
            if (!fieldTypes.TryGetValue(key, out IIcebergType? type))
                throw new InvalidDataException($"Column bound refers to unknown field ID {key}.");
            writer.WriteInt(key);
            writer.WriteBytes(IcebergLiteralSerializer.Serialize(type, value));
        }

        writer.WriteLong(0);
    }

    private static ImmutableDictionary<int, LiteralValue>? ReadNullableLiteralMap(
        ref AvroBinaryReader reader,
        IReadOnlyDictionary<int, IIcebergType> fieldTypes,
        bool skip)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableDictionary<int, LiteralValue>.Builder? values = skip
            ? null
            : ImmutableDictionary.CreateBuilder<int, LiteralValue>();
        while (AvroSerializationUtilities.TryReadArrayBlock(ref reader, out long count))
        {
            for (long i = 0; i < count; i++)
            {
                int key = reader.ReadInt();
                ReadOnlySpan<byte> bytes = reader.ReadBytes();
                if (skip) continue;
                if (!fieldTypes.TryGetValue(key, out IIcebergType? type))
                    throw new InvalidDataException($"Column bound refers to unknown field ID {key}.");
                values!.Add(key, IcebergLiteralSerializer.Deserialize(type, bytes));
            }
        }

        return values?.ToImmutable();
    }

    private static bool ShouldSkip(IReadOnlySet<int>? fieldIds, int fieldId) =>
        fieldIds is not null && !fieldIds.Contains(fieldId);
}
