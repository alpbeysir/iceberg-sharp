using System.Text.Json;
using Avro;
using Avro.File;
using Avro.Generic;
using Avro.IO;
using Iceberg.Net.Misc;
using Iceberg.Net.Rest;

namespace Iceberg.Net.Metadata;

public enum Status
{
    Existing = 0,
    Added = 1,
    Deleted = 2
}

public readonly record struct ManifestEntry
{
    internal const string MetadataSchemaKey = "schema";
    internal const string MetadataSchemaIdKey = "schema-id";
    internal const string MetadataPartitionSpecKey = "partition-spec";
    internal const string MetadataPartitionSpecIdKey = "partition-spec-id";
    internal const string MetadataFormatVersionKey = "format-version";
    internal const string MetadataContentKey = "content";

    private const string SchemaString =
        """{"type":"record","fields":[{"name":"status","field-id":0,"type":"int"},{"name":"snapshot_id","field-id":1,"type":["null","long"],"default":null},{"name":"sequence_number","field-id":3,"type":["null","long"],"default":null},{"name":"file_sequence_number","field-id":4,"type":["null","long"],"default":null},{"name":"data_file","field-id":2,"type":{"type":"record","fields":[{"name":"content","field-id":134,"type":"int"},{"name":"file_path","field-id":100,"type":"string"},{"name":"file_format","field-id":101,"type":"string"},{"name":"partition","field-id":102,"type":{"type":"record","fields":[],"name":"r102"}},{"name":"record_count","field-id":103,"type":"long"},{"name":"file_size_in_bytes","field-id":104,"type":"long"},{"name":"column_sizes","field-id":108,"type":["null",{"type":"array","items":{"type":"record","name":"k117_v118","fields":[{"name":"key","type":"int","field-id":117},{"name":"value","type":"long","field-id":118}]},"logicalType":"map"}],"default":null},{"name":"value_counts","field-id":109,"type":["null",{"type":"array","items":{"type":"record","name":"k119_v120","fields":[{"name":"key","type":"int","field-id":119},{"name":"value","type":"long","field-id":120}]},"logicalType":"map"}],"default":null},{"name":"null_value_counts","field-id":110,"type":["null",{"type":"array","items":{"type":"record","name":"k121_v122","fields":[{"name":"key","type":"int","field-id":121},{"name":"value","type":"long","field-id":122}]},"logicalType":"map"}],"default":null},{"name":"nan_value_counts","field-id":137,"type":["null",{"type":"array","items":{"type":"record","name":"k138_v139","fields":[{"name":"key","type":"int","field-id":138},{"name":"value","type":"long","field-id":139}]},"logicalType":"map"}],"default":null},{"name":"lower_bounds","field-id":125,"type":["null",{"type":"array","items":{"type":"record","name":"k126_v127","fields":[{"name":"key","type":"int","field-id":126},{"name":"value","type":"bytes","field-id":127}]},"logicalType":"map"}],"default":null},{"name":"upper_bounds","field-id":128,"type":["null",{"type":"array","items":{"type":"record","name":"k129_v130","fields":[{"name":"key","type":"int","field-id":129},{"name":"value","type":"bytes","field-id":130}]},"logicalType":"map"}],"default":null},{"name":"key_metadata","field-id":131,"type":["null","bytes"],"default":null},{"name":"split_offsets","field-id":132,"type":["null",{"type":"array","element-id":133,"items":"long"}],"default":null},{"name":"equality_ids","field-id":135,"type":["null",{"type":"array","element-id":136,"items":"long"}],"default":null},{"name":"sort_order_id","field-id":140,"type":["null","int"],"default":null},{"name":"referenced_data_file","field-id":143,"type":["null","string"],"default":null}],"name":"r2"}}],"name":"manifest_entry"}""";


    private static readonly Schema AvroSchema = Schema.Parse(SchemaString);
    public long? SequenceNumber { get; init; }
    public long? FileSequenceNumber { get; init; }
    public required Status Status { get; init; }
    public long? SnapshotId { get; init; }
    public required DataFile DataFile { get; init; }

    public static string GetFileName(Guid guid, int sequence)
    {
        return $"{guid}-m{sequence}.avro";
    }

    public static IFileReader<ManifestEntry> GetReader(Stream stream)
    {
        ManifestEntryV2Reader entryReader = new(AvroSchema, AvroSchema);
        IFileReader<ManifestEntry>? fileReader = DataFileReader<ManifestEntry>.OpenReader(
            stream,
            AvroSchema,
            (_, _) => entryReader);

        // TODO actually support partition spec reading
        // var partitionSpec = fileReader.GetMetaString("partition-spec");
        GenericReader<GenericRecord> partitionSpecReader = new(
            Utils.EmptyPartitionAvroSchema,
            Utils.EmptyPartitionAvroSchema);
        entryReader.PartitionSpecReader = decoder =>
            partitionSpecReader.Read(null, decoder);

        return fileReader;
    }

    public static IFileWriter<ManifestEntry> GetAppender(
        Stream stream,
        Schemas.Schema tableSchema,
        PartitionSpec partitionSpec,
        Content content)
    {
        ManifestEntryV2Writer datumWriter = new()
        {
            PartitionSpecWriter = GetPartitionSpecWriter(partitionSpec)
        };
        IFileWriter<ManifestEntry>? writer = DataFileWriter<ManifestEntry>.OpenWriter(datumWriter, stream, true);
        WriteMetadata(writer, tableSchema, partitionSpec, content);
        return writer;
    }

    private static Action<GenericRecord, Encoder> GetPartitionSpecWriter(PartitionSpec partitionSpec)
    {
        // TODO actually use argument to generate schema
        DefaultWriter partitionSpecWriter = new(Utils.EmptyPartitionAvroSchema);
        return (obj, encoder) => { partitionSpecWriter.Write(Utils.EmptyPartitionAvroSchema, obj, encoder); };
    }


    private static void WriteMetadata(
        IFileWriter<ManifestEntry> writer,
        Schemas.Schema tableSchema,
        PartitionSpec partitionSpec,
        Content content)
    {
        writer.SetMeta(
            MetadataSchemaKey,
            JsonSerializer.SerializeToUtf8Bytes(tableSchema, SourceGenerationContext.Default.Schema));
        writer.SetMeta(MetadataSchemaIdKey, tableSchema.SchemaId.ToString());
        writer.SetMeta(
            MetadataPartitionSpecKey,
            JsonSerializer.SerializeToUtf8Bytes(
                partitionSpec.Fields,
                SourceGenerationContext.Default.ListPartitionField));
        writer.SetMeta(MetadataPartitionSpecIdKey, partitionSpec.SpecId!.ToString());
        writer.SetMeta(MetadataFormatVersionKey, 2.ToString());
        writer.SetMeta(MetadataContentKey, content.ToMetadataString());
    }

    private record ManifestEntryV2Reader(Schema ReaderSchema, Schema WriterSchema)
        : DatumReader<ManifestEntry>
    {
        public Func<Decoder, GenericRecord>? PartitionSpecReader { get; set; }

        public ManifestEntry Read(ManifestEntry reuse, Decoder decoder)
        {
            return new ManifestEntry
            {
                Status = (Status)decoder.ReadInt(),
                SnapshotId = decoder.ReadOptional(d => d.ReadLong()),
                SequenceNumber = decoder.ReadOptional(d => d.ReadLong()),
                FileSequenceNumber = decoder.ReadOptional(d => d.ReadLong()),
                DataFile = DataFile.Read(decoder, PartitionSpecReader!)
            };
        }
    }

    private record ManifestEntryV2Writer : DatumWriter<ManifestEntry>
    {
        public Action<GenericRecord, Encoder>? PartitionSpecWriter { get; init; }

        public void Write(ManifestEntry datum, Encoder encoder)
        {
            // 0: status
            encoder.WriteInt((int)datum.Status);

            // 1: snapshot_id (long | null)
            if (datum.SnapshotId == null)
            {
                encoder.WriteUnionIndex(0);
            }
            else
            {
                encoder.WriteUnionIndex(1);
                encoder.WriteLong(datum.SnapshotId.Value);
            }

            // 2: sequence_number (long | null)
            if (datum.SequenceNumber == null)
            {
                encoder.WriteUnionIndex(0);
            }
            else
            {
                encoder.WriteUnionIndex(1);
                encoder.WriteLong(datum.SequenceNumber.Value);
            }

            // 3: file_sequence_number (long | null)
            if (datum.FileSequenceNumber == null)
            {
                encoder.WriteUnionIndex(0);
            }
            else
            {
                encoder.WriteUnionIndex(1);
                encoder.WriteLong(datum.FileSequenceNumber.Value);
            }

            // 4: data_file (struct)
            DataFile.Write(datum.DataFile, encoder, PartitionSpecWriter!);
        }

        public Schema Schema => new AvroUtils.AvroSchemaWrapper(AvroSchema, SchemaString);
    }
}