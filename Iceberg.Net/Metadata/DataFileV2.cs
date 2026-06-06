using System.Collections.Immutable;
using Avro.Generic;
using Avro.IO;

namespace Iceberg.Net.Metadata;

public enum DataFileContent
{
    Data,
    PositionDeletes,
    EqualityDeletes
}

public readonly record struct DataFile
{
    public required DataFileContent Content { get; init; }
    public ImmutableArray<int>? EqualityIds { get; init; }
    public string? ReferencedDataFile { get; init; }
    public required string FilePath { get; init; }
    public required string FileFormat { get; init; }
    public required GenericRecord Partition { get; init; }
    public required long RecordCount { get; init; }
    public required long FileSizeInBytes { get; init; }
    public ImmutableDictionary<int, long>? ColumnSizes { get; init; }
    public ImmutableDictionary<int, long>? ValueCounts { get; init; }
    public ImmutableDictionary<int, long>? NullValueCounts { get; init; }
    public ImmutableDictionary<int, long>? NanValueCounts { get; init; }
    public ImmutableDictionary<int, byte[]>? LowerBounds { get; init; }
    public ImmutableDictionary<int, byte[]>? UpperBounds { get; init; }
    public byte[]? KeyMetadata { get; init; }
    public ImmutableArray<long>? SplitOffsets { get; init; }
    public int? SortOrderId { get; init; }

    public static DataFile Read(Decoder decoder, Func<Decoder, GenericRecord> partitionSpecReader)
    {
        DataFile file = new()
        {
            Content = (DataFileContent)decoder.ReadInt(),
            FilePath = decoder.ReadString(),
            FileFormat = decoder.ReadString(),
            // TODO handle partition specs
            Partition = partitionSpecReader(decoder),
            RecordCount = decoder.ReadLong(),
            FileSizeInBytes = decoder.ReadLong(),
            ColumnSizes = decoder.ReadMap(d => d.ReadInt(), d => d.ReadLong()),
            ValueCounts = decoder.ReadMap(d => d.ReadInt(), d => d.ReadLong()),
            NullValueCounts = decoder.ReadMap(d => d.ReadInt(), d => d.ReadLong()),
            NanValueCounts = decoder.ReadMap(d => d.ReadInt(), d => d.ReadLong()),
            // distinct counts deprecated
            LowerBounds = decoder.ReadMap<int, byte[]>(d => d.ReadInt(), d => d.ReadBytes()),
            UpperBounds = decoder.ReadMap<int, byte[]>(d => d.ReadInt(), d => d.ReadBytes()),
            KeyMetadata = decoder.ReadOptional(d => d.ReadBytes()),
            SplitOffsets = decoder.ReadOptional(d => d.ReadArray(d2 => d2.ReadLong())),
            EqualityIds = decoder.ReadOptional(d => d.ReadArray(d2 => d2.ReadInt())),
            SortOrderId = decoder.ReadOptional(d => d.ReadInt()),
            ReferencedDataFile = decoder.ReadOptional(d => d.ReadString())
        };

        return file;
    }

    public static void Write(DataFile file, Encoder encoder, Action<GenericRecord, Encoder> partitionSpecWriter)
    {
        encoder.WriteInt((int)file.Content);
        encoder.WriteString(file.FilePath);
        encoder.WriteString(file.FileFormat);

        // TODO handle partition specs
        partitionSpecWriter(file.Partition, encoder);

        encoder.WriteLong(file.RecordCount);
        encoder.WriteLong(file.FileSizeInBytes);

        encoder.WriteOptional(
            file.ColumnSizes,
            static (e, cs) => e.WriteMap(
                cs,
                static (e2, v) => e2.WriteInt(v),
                static (e2, v) => e2.WriteLong(v)));

        encoder.WriteOptional(
            file.ValueCounts,
            static (e, cs) => e.WriteMap(
                cs,
                static (e2, v) => e2.WriteInt(v),
                static (e2, v) => e2.WriteLong(v)));

        encoder.WriteOptional(
            file.NullValueCounts,
            static (e, cs) => e.WriteMap(
                cs,
                static (e2, v) => e2.WriteInt(v),
                static (e2, v) => e2.WriteLong(v)));

        encoder.WriteOptional(
            file.NanValueCounts,
            static (e, cs) => e.WriteMap(
                cs,
                static (e2, v) => e2.WriteInt(v),
                static (e2, v) => e2.WriteLong(v)));

        encoder.WriteOptional(
            file.LowerBounds,
            static (e, cs) => e.WriteMap(
                cs,
                static (e2, v) => e2.WriteInt(v),
                static (e2, v) => e2.WriteBytes(v)));

        encoder.WriteOptional(
            file.UpperBounds,
            static (e, cs) => e.WriteMap(
                cs,
                static (e2, v) => e2.WriteInt(v),
                static (e2, v) => e2.WriteBytes(v)));

        encoder.WriteOptional(file.KeyMetadata, (encoder1, bytes) => encoder1.WriteBytes(bytes));

        encoder.WriteOptionalStruct(
            file.SplitOffsets,
            (encoder1, longs) => encoder1.WriteArray(longs, (e2, v) => e2.WriteLong(v)));


        encoder.WriteOptionalStruct(
            file.EqualityIds,
            (encoder1, array) => encoder1.WriteArray(array, (e2, v) => e2.WriteInt(v)));

        encoder.WriteOptionalStruct(file.SortOrderId, (encoder1, i) => encoder1.WriteInt(i));
        encoder.WriteOptional(file.ReferencedDataFile, (encoder1, i) => encoder1.WriteString(i));
    }
}