using EngineeredWood.Compression;
using EngineeredWood.Parquet;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.EngineeredWoodParquet;

internal static class EngineeredWoodParquetTableProperties
{
    internal static ParquetReadOptions CreateReadOptions(
        TablePropertyResolver properties,
        bool hasNestedFields) =>
        new()
        {
            // EngineeredWood.Parquet 0.1.0 does not slice repetition and definition levels in
            // its multi-batch nested read path. Read nested row groups atomically until that is
            // fixed upstream; flat schemas still respect read.parquet.vectorization.batch-size.
            BatchSize = hasNestedFields
                ? null
                : properties.GetPositiveInt32(
                    TableProperties.ParquetBatchSize,
                    TableProperties.ParquetBatchSizeDefault),
            DecimalOutput = DecimalOutputKind.Decimal128
        };

    internal static ParquetWriteOptions CreateWriteOptions(TablePropertyResolver properties)
    {
        CompressionCodec compression = ParseCompression(properties.GetString(
            TableProperties.ParquetCompression,
            TableProperties.ParquetCompressionDefaultSince140));
        int? compressionLevel = properties.TryGetInt32(
            TableProperties.ParquetCompressionLevel,
            out int configuredCompressionLevel)
            ? configuredCompressionLevel
            : null;

        return new ParquetWriteOptions
        {
            Compression = compression,
            CompressionLevel = compressionLevel is null
                ? null
                : ParseCompressionLevel(configuredCompressionLevel),
            CustomCompressionLevel = compressionLevel,
            DataPageVersion = ParsePageVersion(properties.GetString(
                TableProperties.ParquetPageVersion,
                TableProperties.ParquetPageVersionDefault)),
            DataPageSize = properties.GetPositiveInt32(
                TableProperties.ParquetPageSizeBytes,
                TableProperties.ParquetPageSizeBytesDefault),
            DictionaryPageSizeLimit = properties.GetPositiveInt32(
                TableProperties.ParquetDictSizeBytes,
                TableProperties.ParquetDictSizeBytesDefault),
            RowGroupMaxRows = 256 * 1024,
            RowGroupMaxBytes = properties.GetPositiveInt64(
                TableProperties.ParquetRowGroupSizeBytes,
                TableProperties.ParquetRowGroupSizeBytesDefault)
        };
    }

    private static CompressionCodec ParseCompression(string value) => value.ToLowerInvariant() switch
    {
        "none" or "uncompressed" => CompressionCodec.Uncompressed,
        "snappy" => CompressionCodec.Snappy,
        "gzip" => CompressionCodec.Gzip,
        "brotli" => CompressionCodec.Brotli,
        "zstd" => CompressionCodec.Zstd,
        "lz4" or "lz4_raw" or "lz4-raw" => CompressionCodec.Lz4,
        "lz4_hadoop" or "lz4-hadoop" => CompressionCodec.Lz4Hadoop,
        "lzo" => CompressionCodec.Lzo,
        _ => throw new NotSupportedException(
            $"Table property '{TableProperties.ParquetCompression}' has unsupported codec '{value}'.")
    };

    private static BlockCompressionLevel ParseCompressionLevel(int value) => value switch
    {
        <= 1 => BlockCompressionLevel.Fastest,
        >= 9 => BlockCompressionLevel.SmallestSize,
        _ => BlockCompressionLevel.Optimal
    };

    private static DataPageVersion ParsePageVersion(string value) => value.ToLowerInvariant() switch
    {
        "v1" => DataPageVersion.V1,
        "v2" => DataPageVersion.V2,
        _ => throw new FormatException(
            $"Table property '{TableProperties.ParquetPageVersion}' has invalid value '{value}'; " +
            "expected 'v1' or 'v2'.")
    };
}
