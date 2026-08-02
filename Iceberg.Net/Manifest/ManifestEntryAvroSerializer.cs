using EngineeredWood.Avro;
using EngineeredWood.Avro.Encoding;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Metadata;

internal sealed class ManifestEntryAvroSerializer : IAvroSerializer<ManifestEntry>
{
    private readonly ManifestEntryTypes _types;

    internal ManifestEntryAvroSerializer(Schema tableSchema, PartitionSpec partitionSpec)
    {
        _types = ResolveTypes(tableSchema, partitionSpec);
        Schema = AvroSchemas.FromSchema(
            ManifestSchemas.ManifestEntryFor(partitionSpec, _types.PartitionTypes),
            "manifest_entry");
    }

    public AvroSchema Schema { get; }

    public void Write(AvroBinaryWriter writer, ManifestEntry value)
    {
        writer.WriteInt((int)value.Status);
        AvroSerializationUtilities.WriteNullableLong(writer, value.SnapshotId);
        AvroSerializationUtilities.WriteNullableLong(writer, value.SequenceNumber);
        AvroSerializationUtilities.WriteNullableLong(writer, value.FileSequenceNumber);
        DataFileAvroSerializer.Write(writer, value.DataFile, _types);
    }

    public ManifestEntry Read(ref AvroBinaryReader reader) =>
        new()
        {
            Status = (Status)reader.ReadInt(),
            SnapshotId = AvroSerializationUtilities.ReadNullableLong(ref reader),
            SequenceNumber = AvroSerializationUtilities.ReadNullableLong(ref reader),
            FileSequenceNumber = AvroSerializationUtilities.ReadNullableLong(ref reader),
            DataFile = DataFileAvroSerializer.Read(ref reader, _types)
        };

    private static ManifestEntryTypes ResolveTypes(
        Schema tableSchema,
        PartitionSpec partitionSpec)
    {
        IReadOnlyDictionary<int, IIcebergType> fieldTypes = SchemaUtilities.FieldsById(tableSchema);
        IReadOnlyList<PrimitiveType> partitionTypes =
            SchemaUtilities.PartitionTypes(fieldTypes, partitionSpec);
        return new ManifestEntryTypes(partitionSpec.SpecId, partitionTypes, fieldTypes);
    }
}

internal readonly record struct ManifestEntryTypes(
    int? PartitionSpecId,
    IReadOnlyList<PrimitiveType> PartitionTypes,
    IReadOnlyDictionary<int, IIcebergType> FieldTypes);
