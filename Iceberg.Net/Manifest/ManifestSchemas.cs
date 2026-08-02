using Iceberg.Net.Schemas;

namespace Iceberg.Net.Metadata;

internal static class ManifestSchemas
{
    internal static class FieldIds
    {
        internal static class ManifestList
        {
            internal const int Partitions = 507;
        }

        internal static class DataFile
        {
            internal const int Partition = 102;
            internal const int ColumnSizes = 108;
            internal const int ValueCounts = 109;
            internal const int NullValueCounts = 110;
            internal const int NanValueCounts = 137;
            internal const int LowerBounds = 125;
            internal const int UpperBounds = 128;
            internal const int SplitOffsets = 132;
            internal const int EqualityIds = 135;
        }
    }

    internal static readonly Schema ManifestList = new(
    [
        Field(500, "manifest_path", new PrimitiveType.String(), doc: "Location URI with FS scheme"),
        Field(501, "manifest_length", new PrimitiveType.Long()),
        Field(502, "partition_spec_id", new PrimitiveType.Int()),
        Field(517, "content", new PrimitiveType.Int()),
        Field(515, "sequence_number", new PrimitiveType.Long()),
        Field(516, "min_sequence_number", new PrimitiveType.Long()),
        Field(503, "added_snapshot_id", new PrimitiveType.Long()),
        Field(504, "added_files_count", new PrimitiveType.Int()),
        Field(505, "existing_files_count", new PrimitiveType.Int()),
        Field(506, "deleted_files_count", new PrimitiveType.Int()),
        Field(512, "added_rows_count", new PrimitiveType.Long()),
        Field(513, "existing_rows_count", new PrimitiveType.Long()),
        Field(514, "deleted_rows_count", new PrimitiveType.Long()),
        Field(
            FieldIds.ManifestList.Partitions,
            "partitions",
            new ListType(
                508,
                new StructType(
                [
                    Field(509, "contains_null", new PrimitiveType.Boolean()),
                    Field(518, "contains_nan", new PrimitiveType.Boolean(), false),
                    Field(510, "lower_bound", new PrimitiveType.Binary(), false),
                    Field(511, "upper_bound", new PrimitiveType.Binary(), false)
                ]),
                true),
            false),
        Field(519, "key_metadata", new PrimitiveType.Binary(), false)
    ]);

    internal static Schema ManifestEntryFor(
        PartitionSpec partitionSpec,
        IReadOnlyList<PrimitiveType> partitionTypes)
    {
        List<StructField> partitionFields =
        [
            .. partitionSpec.Fields
                .Select((field, i) =>
                    Field(
                        field.FieldId ??
                        throw new InvalidDataException($"Partition field '{field.Name}' does not have a field ID."),
                        field.Name,
                        partitionTypes[i], false))
        ];

        StructType dataFile = new(
        [
            Field(134, "content", new PrimitiveType.Int()),
            Field(100, "file_path", new PrimitiveType.String()),
            Field(101, "file_format", new PrimitiveType.String()),
            Field(FieldIds.DataFile.Partition, "partition", new StructType(partitionFields)),
            Field(103, "record_count", new PrimitiveType.Long()),
            Field(104, "file_size_in_bytes", new PrimitiveType.Long()),
            Field(FieldIds.DataFile.ColumnSizes, "column_sizes", IntMap(117, 118, new PrimitiveType.Long()), false),
            Field(FieldIds.DataFile.ValueCounts, "value_counts", IntMap(119, 120, new PrimitiveType.Long()), false),
            Field(FieldIds.DataFile.NullValueCounts, "null_value_counts", IntMap(121, 122, new PrimitiveType.Long()), false),
            Field(FieldIds.DataFile.NanValueCounts, "nan_value_counts", IntMap(138, 139, new PrimitiveType.Long()), false),
            Field(FieldIds.DataFile.LowerBounds, "lower_bounds", IntMap(126, 127, new PrimitiveType.Binary()), false),
            Field(FieldIds.DataFile.UpperBounds, "upper_bounds", IntMap(129, 130, new PrimitiveType.Binary()), false),
            Field(131, "key_metadata", new PrimitiveType.Binary(), false),
            Field(FieldIds.DataFile.SplitOffsets, "split_offsets", new ListType(133, new PrimitiveType.Long(), true), false),
            Field(FieldIds.DataFile.EqualityIds, "equality_ids", new ListType(136, new PrimitiveType.Int(), true), false),
            Field(140, "sort_order_id", new PrimitiveType.Int(), false),
            Field(143, "referenced_data_file", new PrimitiveType.String(), false)
        ]);

        return new Schema(
        [
            Field(0, "status", new PrimitiveType.Int()),
            Field(1, "snapshot_id", new PrimitiveType.Long(), false),
            Field(3, "sequence_number", new PrimitiveType.Long(), false),
            Field(4, "file_sequence_number", new PrimitiveType.Long(), false),
            Field(2, "data_file", dataFile)
        ]);
    }

    private static MapType IntMap(int keyId, int valueId, IIcebergType valueType) =>
        new(keyId, new PrimitiveType.Int(), valueId, valueType, true);

    private static StructField Field(
        int id,
        string name,
        IIcebergType type,
        bool required = true,
        string? doc = null) =>
        new(id, name, type, required, doc);
}
