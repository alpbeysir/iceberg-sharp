using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;
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
            new ManifestEntryAvroSerializer(tableSchema, partitionSpec),
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
            new ManifestListEntryAvroSerializer(tableSchema, partitionSpecs),
            codec,
            compressionLevel,
            cancellationToken);
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
        ManifestEntryAvroSerializer serializer = new(tableSchema, partitionSpec);
        while (await reader.ReadBlockAsync(cancellationToken) is { } block)
        {
            int offset = 0;
            for (long i = 0; i < block.objectCount; i++)
            {
                ManifestEntry entry;
                int bytesRead;
                {
                    AvroBinaryReader binaryReader = new(block.data.Span[offset..]);
                    entry = serializer.Read(ref binaryReader);
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
        ManifestListEntryAvroSerializer serializer = new(tableSchema, partitionSpecs);
        while (await reader.ReadBlockAsync(cancellationToken) is { } block)
        {
            int offset = 0;
            for (long i = 0; i < block.objectCount; i++)
            {
                ManifestListEntry entry;
                int bytesRead;
                {
                    AvroBinaryReader binaryReader = new(block.data.Span[offset..]);
                    entry = serializer.Read(ref binaryReader);
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

    private static byte[] Utf8<T>(T value) =>
        Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
}
