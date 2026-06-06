using System.Collections.Immutable;
using Avro;
using Avro.File;
using Avro.Generic;
using Avro.IO;

namespace Iceberg.Net.Metadata;

public enum Content
{
    Data,
    Deletes
}

public readonly record struct ManifestListEntry
{
    private const string SchemaString =
        """{"type": "record", "fields": [{"name": "manifest_path", "field-id": 500, "type": "string", "doc": "Location URI with FS scheme"}, {"name": "manifest_length", "field-id": 501, "type": "long"}, {"name": "partition_spec_id", "field-id": 502, "type": "int"}, {"name": "content", "field-id": 517, "type": "int"}, {"name": "sequence_number", "field-id": 515, "type": "long"}, {"name": "min_sequence_number", "field-id": 516, "type": "long"}, {"name": "added_snapshot_id", "field-id": 503, "type": "long"}, {"name": "added_files_count", "field-id": 504, "type": "int"}, {"name": "existing_files_count", "field-id": 505, "type": "int"}, {"name": "deleted_files_count", "field-id": 506, "type": "int"}, {"name": "added_rows_count", "field-id": 512, "type": "long"}, {"name": "existing_rows_count", "field-id": 513, "type": "long"}, {"name": "deleted_rows_count", "field-id": 514, "type": "long"}, {"name": "partitions", "field-id": 507, "type": ["null", {"type": "array", "element-id": 508, "items": {"type": "record", "fields": [{"name": "contains_null", "field-id": 509, "type": "boolean"}, {"name": "contains_nan", "field-id": 518, "type": ["null", "boolean"], "default": null}, {"name": "lower_bound", "field-id": 510, "type": ["null", "bytes"], "default": null}, {"name": "upper_bound", "field-id": 511, "type": ["null", "bytes"], "default": null}], "name": "r508"}}], "default": null}, {"name": "key_metadata", "field-id": 519, "type": ["null", "bytes"], "default": null}], "name": "manifest_file"}""";

    private const string MetadataSnapshotIdKey = "snapshot-id";
    private const string MetadataParentSnapshotIdKey = "parent-snapshot-id";
    private const string MetadataSequenceNumberKey = "sequence-number";
    private const string MetadataFormatVersionKey = "format-version";

    private static readonly Schema AvroSchema = Schema.Parse(SchemaString);
    public required Content Content { get; init; }
    public required long SequenceNumber { get; init; }
    public required long MinSequenceNumber { get; init; }
    public required int AddedFilesCount { get; init; }
    public required int ExistingFilesCount { get; init; }
    public required int DeletedFilesCount { get; init; }
    public required long AddedRowsCount { get; init; }
    public required long ExistingRowsCount { get; init; }
    public required long DeletedRowsCount { get; init; }

    public required string ManifestPath { get; init; }
    public required long ManifestLength { get; init; }
    public required int PartitionSpecId { get; init; }
    public required long AddedSnapshotId { get; init; }
    public ImmutableArray<FieldSummary>? Partitions { get; init; }
    public byte[]? KeyMetadata { get; init; }

    public static IFileReader<ManifestListEntry> GetReader(Stream stream, bool leaveOpen = true)
    {
        return DataFileReader<ManifestListEntry>.OpenReader(
            stream,
            AvroSchema,
            CreateDatumReader,
            leaveOpen);
    }

    public static IFileWriter<ManifestListEntry> GetAppender(
        Stream stream,
        long snapshotId,
        long? parentSnapshotId,
        long sequenceNumber,
        bool leaveOpen = true)
    {
        IFileWriter<ManifestListEntry>? writer =
            DataFileWriter<ManifestListEntry>.OpenWriter(new ManifestFileV2Writer(), stream, leaveOpen);
        WriteMetadata(writer, snapshotId, parentSnapshotId, sequenceNumber);

        return writer;
    }

    private static void WriteMetadata(
        IFileWriter<ManifestListEntry> writer,
        long snapshotId,
        long? parentSnapshotId,
        long sequenceNumber)
    {
        writer.SetMeta(MetadataSnapshotIdKey, snapshotId.ToString());
        writer.SetMeta(MetadataParentSnapshotIdKey, parentSnapshotId.ToString());
        writer.SetMeta(MetadataSequenceNumberKey, sequenceNumber.ToString());
        writer.SetMeta(MetadataFormatVersionKey, 2.ToString());
    }


    private static DatumReader<ManifestListEntry> CreateDatumReader(
        Schema writerSchema,
        Schema readerSchema)
    {
        return new ManifestFileV2Reader(readerSchema, writerSchema);
    }

    public static string GetFileName(long snapshotId, long sequenceNumber, Guid guid)
    {
        return $"snap-{snapshotId}-{sequenceNumber}-{guid}.avro";
    }

    private record ManifestFileV2Reader(Schema ReaderSchema, Schema WriterSchema)
        : DatumReader<ManifestListEntry>
    {
        public ManifestListEntry Read(ManifestListEntry reuse, Decoder decoder)
        {
            return new ManifestListEntry
            {
                ManifestPath = decoder.ReadString(),
                ManifestLength = decoder.ReadLong(),
                PartitionSpecId = decoder.ReadInt(),
                Content = (Content)decoder.ReadInt(),
                SequenceNumber = decoder.ReadLong(),
                MinSequenceNumber = decoder.ReadLong(),
                AddedSnapshotId = decoder.ReadLong(),
                AddedFilesCount = decoder.ReadInt(),
                ExistingFilesCount = decoder.ReadInt(),
                DeletedFilesCount = decoder.ReadInt(),
                AddedRowsCount = decoder.ReadLong(),
                ExistingRowsCount = decoder.ReadLong(),
                DeletedRowsCount = decoder.ReadLong(),
                Partitions = decoder.ReadOptionalStruct(d => d.ReadArray(FieldSummary.Read)),
                KeyMetadata = decoder.ReadOptional(d => d.ReadBytes())
            };
        }
    }

    private sealed record ManifestFileV2Writer : DatumWriter<ManifestListEntry>
    {
        public void Write(ManifestListEntry datum, Encoder encoder)
        {
            encoder.WriteString(datum.ManifestPath);
            encoder.WriteLong(datum.ManifestLength);
            encoder.WriteInt(datum.PartitionSpecId);
            encoder.WriteInt((int)datum.Content);
            encoder.WriteLong(datum.SequenceNumber);
            encoder.WriteLong(datum.MinSequenceNumber);
            encoder.WriteLong(datum.AddedSnapshotId);
            encoder.WriteInt(datum.AddedFilesCount);
            encoder.WriteInt(datum.ExistingFilesCount);
            encoder.WriteInt(datum.DeletedFilesCount);
            encoder.WriteLong(datum.AddedRowsCount);
            encoder.WriteLong(datum.ExistingRowsCount);
            encoder.WriteLong(datum.DeletedRowsCount);

            encoder.WriteOptionalStruct(
                datum.Partitions,
                (encoder1, array) => encoder1.WriteArray(array, FieldSummary.Write));

            encoder.WriteOptional(datum.KeyMetadata, (encoder1, bytes) => encoder1.WriteBytes(bytes));
        }

        public Schema Schema => new AvroUtils.AvroSchemaWrapper(AvroSchema, SchemaString);
    }
}