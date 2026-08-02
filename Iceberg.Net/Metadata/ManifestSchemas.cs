using Iceberg.Net.Schemas;

namespace Iceberg.Net.Metadata;

internal static class ManifestSchemas
{
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
            507,
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
        List<StructField> partitionFields = new(partitionSpec.Fields.Count);
        for (int i = 0; i < partitionSpec.Fields.Count; i++)
        {
            PartitionField field = partitionSpec.Fields[i];
            partitionFields.Add(Field(
                field.FieldId ?? throw new InvalidDataException(
                    $"Partition field '{field.Name}' does not have a field ID."),
                field.Name,
                partitionTypes[i],
                false));
        }

        StructType dataFile = new(
        [
            Field(134, "content", new PrimitiveType.Int()),
            Field(100, "file_path", new PrimitiveType.String()),
            Field(101, "file_format", new PrimitiveType.String()),
            Field(102, "partition", new StructType(partitionFields)),
            Field(103, "record_count", new PrimitiveType.Long()),
            Field(104, "file_size_in_bytes", new PrimitiveType.Long()),
            Field(108, "column_sizes", IntMap(117, 118, new PrimitiveType.Long()), false),
            Field(109, "value_counts", IntMap(119, 120, new PrimitiveType.Long()), false),
            Field(110, "null_value_counts", IntMap(121, 122, new PrimitiveType.Long()), false),
            Field(137, "nan_value_counts", IntMap(138, 139, new PrimitiveType.Long()), false),
            Field(125, "lower_bounds", IntMap(126, 127, new PrimitiveType.Binary()), false),
            Field(128, "upper_bounds", IntMap(129, 130, new PrimitiveType.Binary()), false),
            Field(131, "key_metadata", new PrimitiveType.Binary(), false),
            Field(132, "split_offsets", new ListType(133, new PrimitiveType.Long(), true), false),
            Field(135, "equality_ids", new ListType(136, new PrimitiveType.Int(), true), false),
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
