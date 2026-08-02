using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;
using Iceberg.Net.Misc;
using Iceberg.Net.Serialization;

namespace Iceberg.Net.Metadata;

internal static class ManifestIO
{
    internal static ManifestWriter<ManifestEntry> CreateManifestWriter(
        Stream stream,
        Schemas.Schema tableSchema,
        PartitionSpec partitionSpec,
        Content content)
    {
        Dictionary<string, byte[]> metadata = new()
        {
            ["schema"] = JsonSerializer.SerializeToUtf8Bytes(
                tableSchema,
                IcebergJsonContext.Default.Schema),
            ["schema-id"] = Utf8(tableSchema.SchemaId),
            ["partition-spec"] = JsonSerializer.SerializeToUtf8Bytes(
                partitionSpec.Fields,
                IcebergJsonContext.Default.ListPartitionField),
            ["partition-spec-id"] = Utf8(partitionSpec.SpecId),
            ["format-version"] = "2"u8.ToArray(),
            ["content"] = Encoding.UTF8.GetBytes(content.ToMetadataString())
        };

        return new ManifestWriter<ManifestEntry>(
            stream,
            ManifestAvroSchemas.ManifestEntry,
            metadata,
            WriteManifestEntry);
    }

    internal static ManifestWriter<ManifestListEntry> CreateManifestListWriter(
        Stream stream,
        long snapshotId,
        long? parentSnapshotId,
        long sequenceNumber)
    {
        Dictionary<string, byte[]> metadata = new()
        {
            ["snapshot-id"] = Utf8(snapshotId),
            ["parent-snapshot-id"] = Utf8(parentSnapshotId),
            ["sequence-number"] = Utf8(sequenceNumber),
            ["format-version"] = [.. "2"u8]
        };

        return new ManifestWriter<ManifestListEntry>(
            stream,
            ManifestAvroSchemas.ManifestList,
            metadata,
            WriteManifestListEntry);
    }

    internal static async Task ReadManifestAsync(
        Stream stream,
        ChannelWriter<ManifestEntry> output,
        Func<ManifestEntry, ManifestEntry>? transform = null,
        CancellationToken cancellationToken = default)
    {
        await using OcfReaderAsync reader = await OcfReaderAsync.OpenAsync(
            stream,
            cancellationToken);
        while (await reader.ReadBlockAsync(cancellationToken) is { } block)
        {
            int offset = 0;
            for (long i = 0; i < block.objectCount; i++)
            {
                ManifestEntry entry;
                int bytesRead;
                {
                    AvroBinaryReader binaryReader = new(block.data.Span[offset..]);
                    entry = ReadManifestEntry(ref binaryReader);
                    bytesRead = binaryReader.Position;
                }

                offset += bytesRead;
                await output.WriteAsync(transform?.Invoke(entry) ?? entry, cancellationToken);
            }
        }
    }

    internal static async Task ReadManifestListAsync(
        Stream stream,
        ChannelWriter<ManifestListEntry> output,
        CancellationToken cancellationToken = default)
    {
        await using OcfReaderAsync reader = await OcfReaderAsync.OpenAsync(
            stream,
            cancellationToken);
        while (await reader.ReadBlockAsync(cancellationToken) is { } block)
        {
            int offset = 0;
            for (long i = 0; i < block.objectCount; i++)
            {
                ManifestListEntry entry;
                int bytesRead;
                {
                    AvroBinaryReader binaryReader = new(block.data.Span[offset..]);
                    entry = ReadManifestListEntry(ref binaryReader);
                    bytesRead = binaryReader.Position;
                }

                offset += bytesRead;
                await output.WriteAsync(entry, cancellationToken);
            }
        }
    }

    private static void WriteManifestEntry(AvroBinaryWriter writer, ManifestEntry entry)
    {
        writer.WriteInt((int)entry.Status);
        WriteNullableLong(writer, entry.SnapshotId);
        WriteNullableLong(writer, entry.SequenceNumber);
        WriteNullableLong(writer, entry.FileSequenceNumber);
        WriteDataFile(writer, entry.DataFile);
    }

    private static ManifestEntry ReadManifestEntry(ref AvroBinaryReader reader)
    {
        return new ManifestEntry
        {
            Status = (Status)reader.ReadInt(),
            SnapshotId = ReadNullableLong(ref reader),
            SequenceNumber = ReadNullableLong(ref reader),
            FileSequenceNumber = ReadNullableLong(ref reader),
            DataFile = ReadDataFile(ref reader)
        };
    }

    private static void WriteDataFile(AvroBinaryWriter writer, DataFile file)
    {
        if (!file.Partition.IsDefaultOrEmpty)
            throw new NotSupportedException("Partitioned manifest entries are not supported yet.");

        writer.WriteInt((int)file.Content);
        writer.WriteString(file.FilePath);
        writer.WriteString(file.FileFormat);
        // The currently supported unpartitioned partition struct has no fields.
        writer.WriteLong(file.RecordCount);
        writer.WriteLong(file.FileSizeInBytes);
        WriteNullableLongMap(writer, file.ColumnSizes);
        WriteNullableLongMap(writer, file.ValueCounts);
        WriteNullableLongMap(writer, file.NullValueCounts);
        WriteNullableLongMap(writer, file.NanValueCounts);
        WriteNullableBytesMap(writer, file.LowerBounds);
        WriteNullableBytesMap(writer, file.UpperBounds);
        WriteNullableBytes(writer, file.KeyMetadata);
        WriteNullableLongArray(writer, file.SplitOffsets);
        WriteNullableIntArray(writer, file.EqualityIds);
        WriteNullableInt(writer, file.SortOrderId);
        WriteNullableString(writer, file.ReferencedDataFile);
    }

    private static DataFile ReadDataFile(ref AvroBinaryReader reader)
    {
        return new DataFile
        {
            Content = (DataFileContent)reader.ReadInt(),
            FilePath = reader.ReadString(),
            FileFormat = reader.ReadString(),
            Partition = [],
            RecordCount = reader.ReadLong(),
            FileSizeInBytes = reader.ReadLong(),
            ColumnSizes = ReadNullableLongMap(ref reader),
            ValueCounts = ReadNullableLongMap(ref reader),
            NullValueCounts = ReadNullableLongMap(ref reader),
            NanValueCounts = ReadNullableLongMap(ref reader),
            LowerBounds = ReadNullableBytesMap(ref reader),
            UpperBounds = ReadNullableBytesMap(ref reader),
            KeyMetadata = ReadNullableBytes(ref reader),
            SplitOffsets = ReadNullableLongArray(ref reader),
            EqualityIds = ReadNullableIntArray(ref reader),
            SortOrderId = ReadNullableInt(ref reader),
            ReferencedDataFile = ReadNullableString(ref reader)
        };
    }

    private static void WriteManifestListEntry(AvroBinaryWriter writer, ManifestListEntry entry)
    {
        writer.WriteString(entry.ManifestPath);
        writer.WriteLong(entry.ManifestLength);
        writer.WriteInt(entry.PartitionSpecId);
        writer.WriteInt((int)entry.Content);
        writer.WriteLong(entry.SequenceNumber);
        writer.WriteLong(entry.MinSequenceNumber);
        writer.WriteLong(entry.AddedSnapshotId);
        writer.WriteInt(entry.AddedFilesCount);
        writer.WriteInt(entry.ExistingFilesCount);
        writer.WriteInt(entry.DeletedFilesCount);
        writer.WriteLong(entry.AddedRowsCount);
        writer.WriteLong(entry.ExistingRowsCount);
        writer.WriteLong(entry.DeletedRowsCount);
        WriteNullableFieldSummaries(writer, entry.Partitions);
        WriteNullableBytes(writer, entry.KeyMetadata);
    }

    private static ManifestListEntry ReadManifestListEntry(ref AvroBinaryReader reader)
    {
        return new ManifestListEntry
        {
            ManifestPath = reader.ReadString(),
            ManifestLength = reader.ReadLong(),
            PartitionSpecId = reader.ReadInt(),
            Content = (Content)reader.ReadInt(),
            SequenceNumber = reader.ReadLong(),
            MinSequenceNumber = reader.ReadLong(),
            AddedSnapshotId = reader.ReadLong(),
            AddedFilesCount = reader.ReadInt(),
            ExistingFilesCount = reader.ReadInt(),
            DeletedFilesCount = reader.ReadInt(),
            AddedRowsCount = reader.ReadLong(),
            ExistingRowsCount = reader.ReadLong(),
            DeletedRowsCount = reader.ReadLong(),
            Partitions = ReadNullableFieldSummaries(ref reader),
            KeyMetadata = ReadNullableBytes(ref reader)
        };
    }

    private static void WriteNullableFieldSummaries(
        AvroBinaryWriter writer,
        ImmutableArray<FieldSummary>? summaries)
    {
        if (summaries is null)
        {
            writer.WriteUnionIndex(0);
            return;
        }

        writer.WriteUnionIndex(1);
        WriteArrayStart(writer, summaries.Value.Length);
        foreach (FieldSummary summary in summaries.Value)
        {
            writer.WriteBoolean(summary.ContainsNull);
            WriteNullableBoolean(writer, summary.ContainsNan);
            WriteNullableBytes(writer, summary.LowerBound);
            WriteNullableBytes(writer, summary.UpperBound);
        }

        writer.WriteLong(0);
    }

    private static ImmutableArray<FieldSummary>? ReadNullableFieldSummaries(
        ref AvroBinaryReader reader)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableArray<FieldSummary>.Builder summaries = ImmutableArray.CreateBuilder<FieldSummary>();
        while (TryReadArrayBlock(ref reader, out long count))
        {
            for (long i = 0; i < count; i++)
            {
                bool containsNull = reader.ReadBoolean();
                bool? containsNan = ReadNullableBoolean(ref reader);
                summaries.Add(new FieldSummary(
                    containsNan,
                    containsNull,
                    ReadNullableBytes(ref reader),
                    ReadNullableBytes(ref reader)));
            }
        }

        return summaries.ToImmutable();
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
        WriteArrayStart(writer, values.Count);
        foreach ((int key, long value) in values)
        {
            writer.WriteInt(key);
            writer.WriteLong(value);
        }

        writer.WriteLong(0);
    }

    private static ImmutableDictionary<int, long>? ReadNullableLongMap(
        ref AvroBinaryReader reader)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableDictionary<int, long>.Builder values = ImmutableDictionary.CreateBuilder<int, long>();
        while (TryReadArrayBlock(ref reader, out long count))
            for (long i = 0; i < count; i++)
                values.Add(reader.ReadInt(), reader.ReadLong());
        return values.ToImmutable();
    }

    private static void WriteNullableBytesMap(
        AvroBinaryWriter writer,
        IReadOnlyDictionary<int, byte[]>? values)
    {
        if (values is null)
        {
            writer.WriteUnionIndex(0);
            return;
        }

        writer.WriteUnionIndex(1);
        WriteArrayStart(writer, values.Count);
        foreach ((int key, byte[] value) in values)
        {
            writer.WriteInt(key);
            writer.WriteBytes(value);
        }

        writer.WriteLong(0);
    }

    private static ImmutableDictionary<int, byte[]>? ReadNullableBytesMap(
        ref AvroBinaryReader reader)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableDictionary<int, byte[]>.Builder values = ImmutableDictionary.CreateBuilder<int, byte[]>();
        while (TryReadArrayBlock(ref reader, out long count))
            for (long i = 0; i < count; i++)
                values.Add(reader.ReadInt(), reader.ReadBytes().ToArray());
        return values.ToImmutable();
    }

    private static void WriteNullableLongArray(
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

    private static ImmutableArray<long>? ReadNullableLongArray(ref AvroBinaryReader reader)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableArray<long>.Builder values = ImmutableArray.CreateBuilder<long>();
        while (TryReadArrayBlock(ref reader, out long count))
            for (long i = 0; i < count; i++)
                values.Add(reader.ReadLong());
        return values.ToImmutable();
    }

    private static void WriteNullableIntArray(
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

    private static ImmutableArray<int>? ReadNullableIntArray(ref AvroBinaryReader reader)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableArray<int>.Builder values = ImmutableArray.CreateBuilder<int>();
        while (TryReadArrayBlock(ref reader, out long count))
            for (long i = 0; i < count; i++)
                values.Add(reader.ReadInt());
        return values.ToImmutable();
    }

    private static bool TryReadArrayBlock(ref AvroBinaryReader reader, out long count)
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

    private static void WriteArrayStart(AvroBinaryWriter writer, int count)
    {
        if (count > 0) writer.WriteLong(count);
    }

    private static void WriteNullableLong(AvroBinaryWriter writer, long? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteLong(value.Value);
    }

    private static long? ReadNullableLong(ref AvroBinaryReader reader)
    {
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadLong();
    }

    private static void WriteNullableInt(AvroBinaryWriter writer, int? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteInt(value.Value);
    }

    private static int? ReadNullableInt(ref AvroBinaryReader reader)
    {
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadInt();
    }

    private static void WriteNullableBoolean(AvroBinaryWriter writer, bool? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteBoolean(value.Value);
    }

    private static bool? ReadNullableBoolean(ref AvroBinaryReader reader)
    {
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadBoolean();
    }

    private static void WriteNullableBytes(AvroBinaryWriter writer, byte[]? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteBytes(value);
    }

    private static byte[]? ReadNullableBytes(ref AvroBinaryReader reader)
    {
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadBytes().ToArray();
    }

    private static void WriteNullableString(AvroBinaryWriter writer, string? value)
    {
        writer.WriteUnionIndex(value is null ? 0 : 1);
        if (value is not null) writer.WriteString(value);
    }

    private static string? ReadNullableString(ref AvroBinaryReader reader)
    {
        return reader.ReadUnionIndex() == 0 ? null : reader.ReadString();
    }

    private static byte[] Utf8<T>(T value)
    {
        return Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
    }
}

internal sealed class ManifestWriter<T> : IDisposable
{
    private readonly OcfWriter _container;
    private readonly AvroBinaryWriter _binaryWriter = new(4096);
    private readonly Action<AvroBinaryWriter, T> _write;
    private bool _disposed;

    internal ManifestWriter(
        Stream stream,
        AvroSchema schema,
        IReadOnlyDictionary<string, byte[]> metadata,
        Action<AvroBinaryWriter, T> write)
    {
        _container = new OcfWriter(stream, AvroCodec.Null);
        _container.WriteHeader(schema, metadata);
        _write = write;
    }

    internal void Append(T value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _binaryWriter.Reset();
        _write(_binaryWriter, value);
        _container.WriteBlock(_binaryWriter.WrittenSpan, 1);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _container.Dispose();
    }
}