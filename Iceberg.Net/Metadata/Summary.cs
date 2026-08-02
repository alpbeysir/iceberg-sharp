using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

[JsonConverter(typeof(SummaryConverter))]
public record Summary
{
    public required SummaryOperation Operation { get; init; }

    public long? AddedDataFiles { get; internal set; }
    public long? DeletedDataFiles { get; internal set; }
    public long? TotalDataFiles { get; internal set; }
    public long? AddedDeleteFiles { get; internal set; }
    public long? AddedEqualityDeleteFiles { get; internal set; }
    public long? RemovedEqualityDeleteFiles { get; internal set; }
    public long? AddedPositionDeleteFiles { get; internal set; }
    public long? RemovedPositionDeleteFiles { get; internal set; }
    public long? AddedDvs { get; internal set; }
    public long? RemovedDvs { get; internal set; }
    public long? RemovedDeleteFiles { get; internal set; }
    public long? TotalDeleteFiles { get; internal set; }
    public long? AddedRecords { get; internal set; }
    public long? DeletedRecords { get; internal set; }
    public long? TotalRecords { get; internal set; }
    public long? AddedFilesSize { get; internal set; }
    public long? RemovedFilesSize { get; internal set; }
    public long? TotalFilesSize { get; internal set; }
    public long? AddedPositionDeletes { get; internal set; }
    public long? RemovedPositionDeletes { get; internal set; }
    public long? TotalPositionDeletes { get; internal set; }
    public long? AddedEqualityDeletes { get; internal set; }
    public long? RemovedEqualityDeletes { get; internal set; }
    public long? TotalEqualityDeletes { get; internal set; }
    public long? DeletedDuplicateFiles { get; internal set; }
    public int? ChangedPartitionCount { get; internal set; }
    public int? ManifestsCreated { get; internal set; }
    public int? ManifestsKept { get; internal set; }
    public int? ManifestsReplaced { get; internal set; }
    public long? EntriesProcessed { get; internal set; }
    public string? WapId { get; internal set; }
    public string? PublishedWapId { get; internal set; }
    public long? SourceSnapshotId { get; internal set; }
    public string? EngineName { get; internal set; }
    public string? EngineVersion { get; internal set; }
}

public class SummaryConverter : JsonConverter<Summary>
{
    public override Summary Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject token.");

        // Using 'default!' because Operation is required and will be set in the loop
        Summary summary = new() { Operation = default! };

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject) return summary;

            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                string propertyName = reader.GetString()!;
                reader.Read();

                switch (propertyName)
                {
                    case "operation":
                        if (Enum.TryParse(reader.GetString(), true, out SummaryOperation op))
                            summary = summary with { Operation = op };
                        break;
                    case "added-data-files": summary.AddedDataFiles = ReadLong(ref reader); break;
                    case "deleted-data-files": summary.DeletedDataFiles = ReadLong(ref reader); break;
                    case "total-data-files": summary.TotalDataFiles = ReadLong(ref reader); break;
                    case "added-delete-files": summary.AddedDeleteFiles = ReadLong(ref reader); break;
                    case "added-equality-delete-files": summary.AddedEqualityDeleteFiles = ReadLong(ref reader); break;
                    case "removed-equality-delete-files":
                        summary.RemovedEqualityDeleteFiles = ReadLong(ref reader); break;
                    case "added-position-delete-files": summary.AddedPositionDeleteFiles = ReadLong(ref reader); break;
                    case "removed-position-delete-files":
                        summary.RemovedPositionDeleteFiles = ReadLong(ref reader); break;
                    case "added-dvs": summary.AddedDvs = ReadLong(ref reader); break;
                    case "removed-dvs": summary.RemovedDvs = ReadLong(ref reader); break;
                    case "removed-delete-files": summary.RemovedDeleteFiles = ReadLong(ref reader); break;
                    case "total-delete-files": summary.TotalDeleteFiles = ReadLong(ref reader); break;
                    case "added-records": summary.AddedRecords = ReadLong(ref reader); break;
                    case "deleted-records": summary.DeletedRecords = ReadLong(ref reader); break;
                    case "total-records": summary.TotalRecords = ReadLong(ref reader); break;
                    case "added-files-size": summary.AddedFilesSize = ReadLong(ref reader); break;
                    case "removed-files-size": summary.RemovedFilesSize = ReadLong(ref reader); break;
                    case "total-files-size": summary.TotalFilesSize = ReadLong(ref reader); break;
                    case "added-position-deletes": summary.AddedPositionDeletes = ReadLong(ref reader); break;
                    case "removed-position-deletes": summary.RemovedPositionDeletes = ReadLong(ref reader); break;
                    case "total-position-deletes": summary.TotalPositionDeletes = ReadLong(ref reader); break;
                    case "added-equality-deletes": summary.AddedEqualityDeletes = ReadLong(ref reader); break;
                    case "removed-equality-deletes": summary.RemovedEqualityDeletes = ReadLong(ref reader); break;
                    case "total-equality-deletes": summary.TotalEqualityDeletes = ReadLong(ref reader); break;
                    case "deleted-duplicate-files": summary.DeletedDuplicateFiles = ReadLong(ref reader); break;
                    case "changed-partition-count": summary.ChangedPartitionCount = ReadInt(ref reader); break;
                    case "manifests-created": summary.ManifestsCreated = ReadInt(ref reader); break;
                    case "manifests-kept": summary.ManifestsKept = ReadInt(ref reader); break;
                    case "manifests-replaced": summary.ManifestsReplaced = ReadInt(ref reader); break;
                    case "entries-processed": summary.EntriesProcessed = ReadLong(ref reader); break;
                    case "wap.id": summary.WapId = reader.GetString(); break;
                    case "published-wap-id": summary.PublishedWapId = reader.GetString(); break;
                    case "source-snapshot-id": summary.SourceSnapshotId = ReadLong(ref reader); break;
                    case "engine-name": summary.EngineName = reader.GetString(); break;
                    case "engine-version": summary.EngineVersion = reader.GetString(); break;
                }
            }
        }

        return summary;
    }

    public override void Write(Utf8JsonWriter writer, Summary value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        writer.WriteString("operation", value.Operation.ToString().ToLowerInvariant());

        // Numeric fields: Write as strings, Omit if null
        WriteLong(writer, "added-data-files", value.AddedDataFiles);
        WriteLong(writer, "deleted-data-files", value.DeletedDataFiles);
        WriteLong(writer, "total-data-files", value.TotalDataFiles);
        WriteLong(writer, "added-delete-files", value.AddedDeleteFiles);
        WriteLong(writer, "added-equality-delete-files", value.AddedEqualityDeleteFiles);
        WriteLong(writer, "removed-equality-delete-files", value.RemovedEqualityDeleteFiles);
        WriteLong(writer, "added-position-delete-files", value.AddedPositionDeleteFiles);
        WriteLong(writer, "removed-position-delete-files", value.RemovedPositionDeleteFiles);
        WriteLong(writer, "added-dvs", value.AddedDvs);
        WriteLong(writer, "removed-dvs", value.RemovedDvs);
        WriteLong(writer, "removed-delete-files", value.RemovedDeleteFiles);
        WriteLong(writer, "total-delete-files", value.TotalDeleteFiles);
        WriteLong(writer, "added-records", value.AddedRecords);
        WriteLong(writer, "deleted-records", value.DeletedRecords);
        WriteLong(writer, "total-records", value.TotalRecords);
        WriteLong(writer, "added-files-size", value.AddedFilesSize);
        WriteLong(writer, "removed-files-size", value.RemovedFilesSize);
        WriteLong(writer, "total-files-size", value.TotalFilesSize);
        WriteLong(writer, "added-position-deletes", value.AddedPositionDeletes);
        WriteLong(writer, "removed-position-deletes", value.RemovedPositionDeletes);
        WriteLong(writer, "total-position-deletes", value.TotalPositionDeletes);
        WriteLong(writer, "added-equality-deletes", value.AddedEqualityDeletes);
        WriteLong(writer, "removed-equality-deletes", value.RemovedEqualityDeletes);
        WriteLong(writer, "total-equality-deletes", value.TotalEqualityDeletes);
        WriteLong(writer, "deleted-duplicate-files", value.DeletedDuplicateFiles);
        WriteInt(writer, "changed-partition-count", value.ChangedPartitionCount);
        WriteInt(writer, "manifests-created", value.ManifestsCreated);
        WriteInt(writer, "manifests-kept", value.ManifestsKept);
        WriteInt(writer, "manifests-replaced", value.ManifestsReplaced);
        WriteLong(writer, "entries-processed", value.EntriesProcessed);
        WriteLong(writer, "source-snapshot-id", value.SourceSnapshotId);

        // String fields: Omit if null
        if (value.WapId != null) writer.WriteString("wap.id", value.WapId);
        if (value.PublishedWapId != null) writer.WriteString("published-wap-id", value.PublishedWapId);
        if (value.EngineName != null) writer.WriteString("engine-name", value.EngineName);
        if (value.EngineVersion != null) writer.WriteString("engine-version", value.EngineVersion);

        writer.WriteEndObject();
    }

    private static long? ReadLong(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.Null) return null;
        if (reader.TokenType == JsonTokenType.Number) return reader.GetInt64();
        if (reader.TokenType == JsonTokenType.String && long.TryParse(
                reader.GetString(),
                CultureInfo.InvariantCulture,
                out long l)) return l;
        return null;
    }

    private static int? ReadInt(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.Null) return null;
        if (reader.TokenType == JsonTokenType.Number) return reader.GetInt32();
        if (reader.TokenType == JsonTokenType.String && int.TryParse(
                reader.GetString(),
                CultureInfo.InvariantCulture,
                out int i)) return i;
        return null;
    }

    private static void WriteLong(Utf8JsonWriter writer, string name, long? value)
    {
        if (value.HasValue) writer.WriteString(name, value.Value.ToString(CultureInfo.InvariantCulture));
    }

    private static void WriteInt(Utf8JsonWriter writer, string name, int? value)
    {
        if (value.HasValue) writer.WriteString(name, value.Value.ToString(CultureInfo.InvariantCulture));
    }
}