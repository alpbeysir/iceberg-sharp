using ParquetSharp;
using ParquetSharp.Arrow;

namespace Iceberg.Net.Catalog;

internal static class ParquetTableProperties
{
    public static WriterProperties CreateWriterProperties(TablePropertyResolver properties)
    {
        string fileFormat = properties.GetString(
            TableProperties.DefaultFileFormat,
            TableProperties.DefaultFileFormatDefault);
        if (!fileFormat.Equals("parquet", StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException(
                $"Table property '{TableProperties.DefaultFileFormat}' is '{fileFormat}', " +
                "but this client currently supports writing only Parquet data files.");

        using WriterPropertiesBuilder builder = new();
        // Preserve the existing row-count cap. This is deliberately not derived from
        // write.parquet.row-group-size-bytes, which is a byte target rather than a row count.
        builder.MaxRowGroupLength(256 * 1024);
        builder.Compression(ParseCompression(properties.GetString(
            TableProperties.ParquetCompression,
            TableProperties.ParquetCompressionDefaultSince140)));

        if (properties.TryGetInt32(TableProperties.ParquetCompressionLevel, out int compressionLevel))
            builder.CompressionLevel(compressionLevel);

        builder.DataPagesize(GetPositiveInt32(
            properties,
            TableProperties.ParquetPageSizeBytes,
            TableProperties.ParquetPageSizeBytesDefault));
        builder.DictionaryPagesizeLimit(GetPositiveInt32(
            properties,
            TableProperties.ParquetDictSizeBytes,
            TableProperties.ParquetDictSizeBytesDefault));
        builder.DataPageVersion(ParsePageVersion(properties.GetString(
            TableProperties.ParquetPageVersion,
            TableProperties.ParquetPageVersionDefault)));

        ApplyColumnSettings(
            builder,
            properties,
            TableProperties.ParquetDictEncodingEnabledColumnPrefix,
            static (configuredBuilder, column, enabled) =>
            {
                if (enabled) configuredBuilder.EnableDictionary(column);
                else configuredBuilder.DisableDictionary(column);
            });
        ApplyColumnSettings(
            builder,
            properties,
            TableProperties.ParquetColumnStatsEnabledPrefix,
            static (configuredBuilder, column, enabled) =>
            {
                if (enabled) configuredBuilder.EnableStatistics(column);
                else configuredBuilder.DisableStatistics(column);
            });

        // Iceberg requires decimal precision <= 9 to use INT32, precision <= 18 to use INT64,
        // and larger precisions to use the minimum-width FIXED_LEN_BYTE_ARRAY.
        builder.EnableStoreDecimalAsInteger();
        return builder.Build();
    }

    public static void ApplyReaderProperties(
        ArrowReaderProperties readerProperties,
        TablePropertyResolver properties)
    {
        readerProperties.BatchSize = GetPositiveInt32(
            properties,
            TableProperties.ParquetBatchSize,
            TableProperties.ParquetBatchSizeDefault);
    }

    private static int GetPositiveInt32(
        TablePropertyResolver properties,
        string property,
        int defaultValue)
    {
        int value = properties.GetInt32(property, defaultValue);
        return value > 0
            ? value
            : throw new FormatException(
                $"Table property '{property}' has invalid value '{value}'; expected a positive integer.");
    }

    private static Compression ParseCompression(string value)
    {
        return value.ToLowerInvariant() switch
        {
            "none" or "uncompressed" => Compression.Uncompressed,
            "snappy" => Compression.Snappy,
            "gzip" => Compression.Gzip,
            "brotli" => Compression.Brotli,
            "zstd" => Compression.Zstd,
            "lz4" or "lz4_raw" or "lz4-raw" => Compression.Lz4,
            "lz4_frame" or "lz4-frame" => Compression.Lz4Frame,
            "lz4_hadoop" or "lz4-hadoop" => Compression.Lz4Hadoop,
            "lzo" => Compression.Lzo,
            "bz2" or "bzip2" => Compression.Bz2,
            _ => throw new NotSupportedException(
                $"Table property '{TableProperties.ParquetCompression}' has unsupported codec '{value}'.")
        };
    }

    private static ParquetDataPageVersion ParsePageVersion(string value)
    {
        return value.ToLowerInvariant() switch
        {
            "v1" => ParquetDataPageVersion.V1,
            "v2" => ParquetDataPageVersion.V2,
            _ => throw new FormatException(
                $"Table property '{TableProperties.ParquetPageVersion}' has invalid value '{value}'; " +
                "expected 'v1' or 'v2'.")
        };
    }

    private static void ApplyColumnSettings(
        WriterPropertiesBuilder builder,
        TablePropertyResolver properties,
        string prefix,
        Action<WriterPropertiesBuilder, string, bool> apply)
    {
        foreach (KeyValuePair<string, string> setting in properties.GetByPrefix(prefix))
        {
            if (string.IsNullOrEmpty(setting.Key))
                throw new FormatException(
                    $"Table property '{prefix}' must include a column path after the prefix.");

            bool enabled = properties.GetBoolean(prefix + setting.Key, defaultValue: false);
            apply(builder, setting.Key, enabled);
        }
    }
}
