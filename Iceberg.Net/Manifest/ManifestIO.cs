using System.Collections.Immutable;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Expressions;
using Iceberg.Net.Catalog;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Misc;
using Iceberg.Net.Schemas;
using Iceberg.Net.Serialization;

namespace Iceberg.Net.Metadata;

internal static class ManifestIO
{
    internal static ValueTask<ManifestWriter<ManifestEntry>> CreateManifestWriterAsync(
        Stream stream,
        Schema tableSchema,
        PartitionSpec partitionSpec,
        Content content,
        TablePropertyResolver properties,
        CancellationToken cancellationToken = default)
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

        (AvroCodec codec, int? compressionLevel) = GetCompression(properties);
        return ManifestWriter<ManifestEntry>.CreateAsync(
            stream,
            metadata,
            new ManifestEntryAvroSerialization(tableSchema, partitionSpec),
            codec,
            compressionLevel,
            cancellationToken);
    }

    internal static ValueTask<ManifestWriter<ManifestListEntry>> CreateManifestListWriterAsync(
        Stream stream,
        long snapshotId,
        long? parentSnapshotId,
        long sequenceNumber,
        Schema tableSchema,
        IReadOnlyList<PartitionSpec> partitionSpecs,
        TablePropertyResolver properties,
        CancellationToken cancellationToken = default)
    {
        Dictionary<string, byte[]> metadata = new()
        {
            ["snapshot-id"] = Utf8(snapshotId),
            ["parent-snapshot-id"] = Utf8(parentSnapshotId),
            ["sequence-number"] = Utf8(sequenceNumber),
            ["format-version"] = [.. "2"u8]
        };

        (AvroCodec codec, int? compressionLevel) = GetCompression(properties);
        return ManifestWriter<ManifestListEntry>.CreateAsync(
            stream,
            metadata,
            new ManifestListEntryAvroSerialization(tableSchema, partitionSpecs),
            codec,
            compressionLevel,
            cancellationToken);
    }

    private static (AvroCodec Codec, int? CompressionLevel) GetCompression(
        TablePropertyResolver properties)
    {
        string configuredCodec = properties.GetString(
            TableProperties.ManifestCompression,
            TableProperties.ManifestCompressionDefault);
        AvroCodec codec = configuredCodec.ToLowerInvariant() switch
        {
            "uncompressed" => AvroCodec.Null,
            "snappy" => AvroCodec.Snappy,
            "gzip" => AvroCodec.Deflate,
            "zstd" => AvroCodec.Zstandard,
            _ => throw new NotSupportedException(
                $"Table property '{TableProperties.ManifestCompression}' has unsupported codec " +
                $"'{configuredCodec}'.")
        };

        if (codec is not (AvroCodec.Deflate or AvroCodec.Zstandard))
            return (codec, null);

        int defaultLevel = codec == AvroCodec.Deflate ? 9 : 1;
        int compressionLevel = properties.GetInt32(
            TableProperties.ManifestCompressionLevel,
            defaultLevel);
        if (codec == AvroCodec.Deflate && compressionLevel is < 0 or > 9)
            throw new FormatException(
                $"Table property '{TableProperties.ManifestCompressionLevel}' has invalid value " +
                $"'{compressionLevel}'; expected an integer from 0 through 9.");
        return (codec, compressionLevel);
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
        (Schema tableSchema, PartitionSpec partitionSpec) = ReadManifestContext(reader.Metadata);
        ManifestEntryAvroSerialization serialization = new(tableSchema, partitionSpec);
        while (await reader.ReadBlockAsync(cancellationToken) is { } block)
        {
            int offset = 0;
            for (long i = 0; i < block.objectCount; i++)
            {
                ManifestEntry entry;
                int bytesRead;
                {
                    AvroBinaryReader binaryReader = new(block.data.Span[offset..]);
                    entry = serialization.Read(ref binaryReader);
                    bytesRead = binaryReader.Position;
                }

                offset += bytesRead;
                await PipelineMetrics.WriteAsync(
                    output,
                    transform?.Invoke(entry) ?? entry,
                    PipelineStage.ManifestRead,
                    cancellationToken);
            }
        }
    }

    internal static async Task ReadManifestListAsync(
        Stream stream,
        ChannelWriter<ManifestListEntry> output,
        Schema tableSchema,
        IReadOnlyList<PartitionSpec> partitionSpecs,
        CancellationToken cancellationToken = default)
    {
        await using OcfReaderAsync reader = await OcfReaderAsync.OpenAsync(
            stream,
            cancellationToken);
        ManifestListEntryAvroSerialization serialization = new(tableSchema, partitionSpecs);
        while (await reader.ReadBlockAsync(cancellationToken) is { } block)
        {
            int offset = 0;
            for (long i = 0; i < block.objectCount; i++)
            {
                ManifestListEntry entry;
                int bytesRead;
                {
                    AvroBinaryReader binaryReader = new(block.data.Span[offset..]);
                    entry = serialization.Read(ref binaryReader);
                    bytesRead = binaryReader.Position;
                }

                offset += bytesRead;
                await PipelineMetrics.WriteAsync(
                    output,
                    entry,
                    PipelineStage.ManifestListRead,
                    cancellationToken);
            }
        }
    }

    private static void WriteManifestEntry(
        AvroBinaryWriter writer,
        ManifestEntry entry,
        ManifestEntryTypes types)
    {
        writer.WriteInt((int)entry.Status);
        WriteNullableLong(writer, entry.SnapshotId);
        WriteNullableLong(writer, entry.SequenceNumber);
        WriteNullableLong(writer, entry.FileSequenceNumber);
        WriteDataFile(writer, entry.DataFile, types);
    }

    private static ManifestEntry ReadManifestEntry(
        ref AvroBinaryReader reader,
        ManifestEntryTypes types)
    {
        return new ManifestEntry
        {
            Status = (Status)reader.ReadInt(),
            SnapshotId = ReadNullableLong(ref reader),
            SequenceNumber = ReadNullableLong(ref reader),
            FileSequenceNumber = ReadNullableLong(ref reader),
            DataFile = ReadDataFile(ref reader, types)
        };
    }

    private static void WriteDataFile(
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
            WritePartitionValue(writer, partitionTypes[i], file.Partition[i]);
        writer.WriteLong(file.RecordCount);
        writer.WriteLong(file.FileSizeInBytes);
        WriteNullableLongMap(writer, file.ColumnSizes);
        WriteNullableLongMap(writer, file.ValueCounts);
        WriteNullableLongMap(writer, file.NullValueCounts);
        WriteNullableLongMap(writer, file.NanValueCounts);
        WriteNullableLiteralMap(writer, file.LowerBounds, types.FieldTypes);
        WriteNullableLiteralMap(writer, file.UpperBounds, types.FieldTypes);
        WriteNullableBytes(writer, file.KeyMetadata);
        WriteNullableLongArray(writer, file.SplitOffsets);
        WriteNullableIntArray(writer, file.EqualityIds);
        WriteNullableInt(writer, file.SortOrderId);
        WriteNullableString(writer, file.ReferencedDataFile);
    }

    private static DataFile ReadDataFile(
        ref AvroBinaryReader reader,
        ManifestEntryTypes types)
    {
        IReadOnlyList<PrimitiveType> partitionTypes = types.PartitionTypes;

        DataFileContent content = (DataFileContent)reader.ReadInt();
        string filePath = reader.ReadString();
        string fileFormat = reader.ReadString();
        ImmutableArray<LiteralValue?>.Builder partition = ImmutableArray.CreateBuilder<LiteralValue?>(
            partitionTypes.Count);
        foreach (PrimitiveType type in partitionTypes)
            partition.Add(ReadPartitionValue(ref reader, type));

        return new DataFile
        {
            Content = content,
            FilePath = filePath,
            FileFormat = fileFormat,
            Partition = partition.MoveToImmutable(),
            RecordCount = reader.ReadLong(),
            FileSizeInBytes = reader.ReadLong(),
            ColumnSizes = ReadNullableLongMap(ref reader),
            ValueCounts = ReadNullableLongMap(ref reader),
            NullValueCounts = ReadNullableLongMap(ref reader),
            NanValueCounts = ReadNullableLongMap(ref reader),
            LowerBounds = ReadNullableLiteralMap(ref reader, types.FieldTypes),
            UpperBounds = ReadNullableLiteralMap(ref reader, types.FieldTypes),
            KeyMetadata = ReadNullableBytes(ref reader),
            SplitOffsets = ReadNullableLongArray(ref reader),
            EqualityIds = ReadNullableIntArray(ref reader),
            SortOrderId = ReadNullableInt(ref reader),
            ReferencedDataFile = ReadNullableString(ref reader)
        };
    }

    private static void WriteManifestListEntry(
        AvroBinaryWriter writer,
        ManifestListEntry entry,
        IReadOnlyList<PrimitiveType> partitionTypes)
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
        WriteNullableFieldSummaries(writer, entry.Partitions, partitionTypes);
        WriteNullableBytes(writer, entry.KeyMetadata);
    }

    private static ManifestListEntry ReadManifestListEntry(
        ref AvroBinaryReader reader,
        IReadOnlyDictionary<int, IReadOnlyList<PrimitiveType>> partitionTypesBySpecId)
    {
        string manifestPath = reader.ReadString();
        long manifestLength = reader.ReadLong();
        int partitionSpecId = reader.ReadInt();
        IReadOnlyList<PrimitiveType> partitionTypes =
            ResolvePartitionTypes(partitionTypesBySpecId, partitionSpecId);

        return new ManifestListEntry
        {
            ManifestPath = manifestPath,
            ManifestLength = manifestLength,
            PartitionSpecId = partitionSpecId,
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
            Partitions = ReadNullableFieldSummaries(ref reader, partitionTypes),
            KeyMetadata = ReadNullableBytes(ref reader)
        };
    }

    private static void WriteNullableFieldSummaries(
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
        WriteArrayStart(writer, summaries.Value.Length);
        for (int i = 0; i < summaries.Value.Length; i++)
        {
            FieldSummary summary = summaries.Value[i];
            writer.WriteBoolean(summary.ContainsNull);
            WriteNullableBoolean(writer, summary.ContainsNan);
            WriteNullableLiteral(writer, summary.LowerBound, partitionTypes[i]);
            WriteNullableLiteral(writer, summary.UpperBound, partitionTypes[i]);
        }

        writer.WriteLong(0);
    }

    private static ImmutableArray<FieldSummary>? ReadNullableFieldSummaries(
        ref AvroBinaryReader reader,
        IReadOnlyList<PrimitiveType> partitionTypes)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableArray<FieldSummary>.Builder summaries = ImmutableArray.CreateBuilder<FieldSummary>();
        int fieldIndex = 0;
        while (TryReadArrayBlock(ref reader, out long count))
        {
            for (long i = 0; i < count; i++)
            {
                bool containsNull = reader.ReadBoolean();
                bool? containsNan = ReadNullableBoolean(ref reader);
                if (fieldIndex >= partitionTypes.Count)
                    throw new InvalidDataException(
                        "Manifest contains more partition summaries than its partition spec.");
                PrimitiveType type = partitionTypes[fieldIndex++];
                summaries.Add(new FieldSummary(
                    containsNan,
                    containsNull,
                    ReadNullableLiteral(ref reader, type),
                    ReadNullableLiteral(ref reader, type)));
            }
        }

        if (fieldIndex != partitionTypes.Count)
            throw new InvalidDataException(
                $"Manifest contains {fieldIndex} partition summaries, but its partition spec contains " +
                $"{partitionTypes.Count} fields.");

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
        WriteArrayStart(writer, values.Count);
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
        IReadOnlyDictionary<int, IIcebergType> fieldTypes)
    {
        if (reader.ReadUnionIndex() == 0) return null;

        ImmutableDictionary<int, LiteralValue>.Builder values =
            ImmutableDictionary.CreateBuilder<int, LiteralValue>();
        while (TryReadArrayBlock(ref reader, out long count))
        {
            for (long i = 0; i < count; i++)
            {
                int key = reader.ReadInt();
                ReadOnlySpan<byte> bytes = reader.ReadBytes();
                if (!fieldTypes.TryGetValue(key, out IIcebergType? type))
                    throw new InvalidDataException($"Column bound refers to unknown field ID {key}.");
                values.Add(key, IcebergLiteralSerializer.Deserialize(type, bytes));
            }
        }

        return values.ToImmutable();
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
        IIcebergType type) =>
        reader.ReadUnionIndex() == 0
            ? (LiteralValue?)null
            : IcebergLiteralSerializer.Deserialize(type, reader.ReadBytes());

    private static void WritePartitionValue(
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

    private static LiteralValue? ReadPartitionValue(
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

    private static (Schema Schema, PartitionSpec PartitionSpec) ReadManifestContext(
        IReadOnlyDictionary<string, byte[]> metadata)
    {
        if (!metadata.TryGetValue("schema", out byte[]? schemaJson))
            throw new InvalidDataException("Manifest metadata does not contain an Iceberg schema.");
        if (!metadata.TryGetValue("partition-spec", out byte[]? partitionSpecJson))
            throw new InvalidDataException("Manifest metadata does not contain an Iceberg partition spec.");

        Schema schema = JsonSerializer.Deserialize(
                            schemaJson,
                            IcebergJsonContext.Default.Schema)
                        ?? throw new InvalidDataException("Manifest contains an invalid Iceberg schema.");
        List<PartitionField> fields = JsonSerializer.Deserialize(
                                          partitionSpecJson,
                                          IcebergJsonContext.Default.ListPartitionField)
                                      ?? throw new InvalidDataException(
                                          "Manifest contains an invalid Iceberg partition spec.");

        int? specId = metadata.TryGetValue("partition-spec-id", out byte[]? specIdBytes) &&
                      int.TryParse(
                          Encoding.UTF8.GetString(specIdBytes),
                          NumberStyles.Integer,
                          CultureInfo.InvariantCulture,
                          out int parsedSpecId)
            ? parsedSpecId
            : null;
        return (schema, new PartitionSpec(fields, specId));
    }

    private static ManifestEntryTypes ResolveManifestEntryPartitioning(
        Schema tableSchema,
        PartitionSpec partitionSpec)
    {
        IReadOnlyDictionary<int, IIcebergType> fieldTypes = SchemaUtilities.FieldsById(tableSchema);
        IReadOnlyList<PrimitiveType> partitionTypes =
            SchemaUtilities.PartitionTypes(fieldTypes, partitionSpec);
        return new ManifestEntryTypes(partitionSpec.SpecId, partitionTypes, fieldTypes);
    }

    private static Dictionary<int, IReadOnlyList<PrimitiveType>> ResolvePartitionTypesBySpecId(
        Schema tableSchema,
        IReadOnlyList<PartitionSpec> partitionSpecs)
    {
        IReadOnlyDictionary<int, IIcebergType> fieldTypes = SchemaUtilities.FieldsById(tableSchema);
        Dictionary<int, IReadOnlyList<PrimitiveType>> result = new(partitionSpecs.Count);
        foreach (PartitionSpec partitionSpec in partitionSpecs)
        {
            int specId = partitionSpec.SpecId ?? throw new InvalidDataException(
                "A table partition spec does not have a spec ID.");
            result.TryAdd(specId, SchemaUtilities.PartitionTypes(fieldTypes, partitionSpec));
        }

        return result;
    }

    private static IReadOnlyList<PrimitiveType> ResolvePartitionTypes(
        IReadOnlyDictionary<int, IReadOnlyList<PrimitiveType>> partitionTypesBySpecId,
        int partitionSpecId)
    {
        return partitionTypesBySpecId.TryGetValue(partitionSpecId, out IReadOnlyList<PrimitiveType>? types)
            ? types
            : throw new InvalidDataException(
                $"Manifest refers to unknown partition spec ID {partitionSpecId}.");
    }

    private static byte[] Utf8<T>(T value)
    {
        return Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
    }

    private readonly record struct ManifestEntryTypes(
        int? PartitionSpecId,
        IReadOnlyList<PrimitiveType> PartitionTypes,
        IReadOnlyDictionary<int, IIcebergType> FieldTypes);

    private sealed class ManifestEntryAvroSerialization : IAvroSerializer<ManifestEntry>
    {
        private readonly ManifestEntryTypes _types;

        internal ManifestEntryAvroSerialization(Schema tableSchema, PartitionSpec partitionSpec)
        {
            _types = ResolveManifestEntryPartitioning(tableSchema, partitionSpec);
            Schema = AvroSchemas.FromSchema(
                ManifestSchemas.ManifestEntryFor(partitionSpec, _types.PartitionTypes),
                "manifest_entry");
        }

        public AvroSchema Schema { get; }

        public void Write(AvroBinaryWriter writer, ManifestEntry value) =>
            WriteManifestEntry(writer, value, _types);

        public ManifestEntry Read(ref AvroBinaryReader reader) =>
            ReadManifestEntry(ref reader, _types);
    }

    private sealed class ManifestListEntryAvroSerialization : IAvroSerializer<ManifestListEntry>
    {
        private readonly IReadOnlyDictionary<int, IReadOnlyList<PrimitiveType>> _partitionTypesBySpecId;

        internal ManifestListEntryAvroSerialization(
            Schema tableSchema,
            IReadOnlyList<PartitionSpec> partitionSpecs)
        {
            _partitionTypesBySpecId = ResolvePartitionTypesBySpecId(tableSchema, partitionSpecs);
        }

        public AvroSchema Schema { get; } =
            AvroSchemas.FromSchema(ManifestSchemas.ManifestList, "manifest_file");

        public void Write(AvroBinaryWriter writer, ManifestListEntry value) =>
            WriteManifestListEntry(
                writer,
                value,
                ResolvePartitionTypes(_partitionTypesBySpecId, value.PartitionSpecId));

        public ManifestListEntry Read(ref AvroBinaryReader reader) =>
            ReadManifestListEntry(ref reader, _partitionTypesBySpecId);
    }
}
