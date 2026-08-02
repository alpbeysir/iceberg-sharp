using EngineeredWood.Avro;
using EngineeredWood.Avro.Encoding;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Metadata;

internal sealed class ManifestListEntryAvroSerializer : IAvroSerializer<ManifestListEntry>
{
    private readonly IReadOnlyDictionary<int, IReadOnlyList<PrimitiveType>> _partitionTypesBySpecId;

    internal ManifestListEntryAvroSerializer(
        Schema tableSchema,
        IReadOnlyList<PartitionSpec> partitionSpecs)
    {
        _partitionTypesBySpecId = ResolvePartitionTypesBySpecId(tableSchema, partitionSpecs);
    }

    public AvroSchema Schema { get; } =
        AvroSchemas.FromSchema(ManifestSchemas.ManifestList, "manifest_file");

    public void Write(AvroBinaryWriter writer, ManifestListEntry value)
    {
        IReadOnlyList<PrimitiveType> partitionTypes = ResolvePartitionTypes(value.PartitionSpecId);
        writer.WriteString(value.ManifestPath);
        writer.WriteLong(value.ManifestLength);
        writer.WriteInt(value.PartitionSpecId);
        writer.WriteInt((int)value.Content);
        writer.WriteLong(value.SequenceNumber);
        writer.WriteLong(value.MinSequenceNumber);
        writer.WriteLong(value.AddedSnapshotId);
        writer.WriteInt(value.AddedFilesCount);
        writer.WriteInt(value.ExistingFilesCount);
        writer.WriteInt(value.DeletedFilesCount);
        writer.WriteLong(value.AddedRowsCount);
        writer.WriteLong(value.ExistingRowsCount);
        writer.WriteLong(value.DeletedRowsCount);
        FieldSummaryAvroSerializer.WriteNullableArray(writer, value.Partitions, partitionTypes);
        AvroSerializationUtilities.WriteNullableBytes(writer, value.KeyMetadata);
    }

    public ManifestListEntry Read(ref AvroBinaryReader reader)
    {
        string manifestPath = reader.ReadString();
        long manifestLength = reader.ReadLong();
        int partitionSpecId = reader.ReadInt();
        IReadOnlyList<PrimitiveType> partitionTypes = ResolvePartitionTypes(partitionSpecId);

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
            Partitions = FieldSummaryAvroSerializer.ReadNullableArray(ref reader, partitionTypes),
            KeyMetadata = AvroSerializationUtilities.ReadNullableBytes(ref reader)
        };
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

    private IReadOnlyList<PrimitiveType> ResolvePartitionTypes(int partitionSpecId) =>
        _partitionTypesBySpecId.TryGetValue(partitionSpecId, out IReadOnlyList<PrimitiveType>? types)
            ? types
            : throw new InvalidDataException(
                $"Manifest refers to unknown partition spec ID {partitionSpecId}.");
}
