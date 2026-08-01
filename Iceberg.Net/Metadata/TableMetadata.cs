using System.Collections.Immutable;
using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Iceberg.Net.Rest;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Schemas;

// ReSharper disable CollectionNeverUpdated.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable InconsistentNaming

namespace Iceberg.Net.Metadata;

public record TableMetadata
{
    private Dictionary<long, Snapshot> _snapshotsById
    {
        get
        {
            field ??= Snapshots.ToDictionary(snapshot => snapshot.SnapshotId);
            return field;
        }
    }

    private Dictionary<int, Schema> _schemasById
    {
        get
        {
            field ??= Schemas.DistinctBy(schema => schema.SchemaId!.Value)
                .ToDictionary(schema => schema.SchemaId!.Value);
            return field;
        }
    }

    [JsonInclude]
    [JsonPropertyName("format-version")]
    public required int FormatVersion { get; init; }

    [JsonInclude]
    [JsonPropertyName("table-uuid")]
    public required string TableUuid { get; init; }

    [JsonInclude]
    [JsonPropertyName("location")]
    public required string Location { get; init; }

    [JsonInclude]
    [JsonPropertyName("last-updated-ms")]
    public long? LastUpdatedMs { get; internal set; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    [JsonInclude]
    [JsonPropertyName("next-row-id")]
    public long? NextRowId { get; internal set; }

    [JsonInclude]
    [JsonPropertyName("current-schema-id")]
    public int? CurrentSchemaId { get; internal set; }

    [JsonInclude]
    [JsonPropertyName("last-column-id")]
    public int? LastColumnId { get; internal set; }

    [JsonInclude]
    [JsonPropertyName("default-spec-id")]
    public int? DefaultSpecId { get; internal set; }

    [JsonInclude]
    [JsonPropertyName("last-partition-id")]
    public int? LastPartitionId { get; internal set; }

    [JsonInclude]
    [JsonPropertyName("default-sort-order-id")]
    public int? DefaultSortOrderId { get; internal set; }

    [JsonInclude]
    [JsonPropertyName("current-snapshot-id")]
    public long? CurrentSnapshotId { get; internal set; }

    [JsonInclude]
    [JsonPropertyName("last-sequence-number")]
    public long? LastSequenceNumber { get; internal set; }

    [JsonInclude]
    [JsonPropertyName("properties")]
    internal Dictionary<string, string> _properties { get; set; } = [];

    public IReadOnlyDictionary<string, string> Properties => _properties;

    [JsonInclude]
    [JsonPropertyName("schemas")]
    public List<Schema> _schemas { get; set; } = [];

    public IReadOnlyList<Schema> Schemas => _schemas;

    [JsonInclude]
    [JsonPropertyName("partition-specs")]
    internal List<PartitionSpec> _partitionSpecs { get; set; } = [];

    public IReadOnlyList<PartitionSpec> PartitionSpecs => _partitionSpecs;

    [JsonInclude]
    [JsonPropertyName("sort-orders")]
    internal List<SortOrder> _sortOrders { get; set; } = [];

    public IReadOnlyList<SortOrder> SortOrders => _sortOrders;

    [JsonInclude]
    [JsonPropertyName("encryption-keys")]
    internal List<EncryptedKey> _encryptionKeys { get; set; } = [];

    public IReadOnlyList<EncryptedKey> EncryptionKeys => _encryptionKeys;

    [JsonInclude]
    [JsonPropertyName("snapshots")]
    internal List<Snapshot> _snapshots { get; set; } = [];

    public IReadOnlyList<Snapshot> Snapshots => _snapshots;

    [JsonInclude]
    [JsonPropertyName("refs")]
    internal Dictionary<string, SnapshotReference> _refs { get; set; } = [];

    public IReadOnlyDictionary<string, SnapshotReference> Refs => _refs;

    [JsonInclude]
    [JsonPropertyName("snapshot-log")]
    internal List<SnapshotLogEntry> _snapshotLog { get; set; } = [];

    public IReadOnlyList<SnapshotLogEntry> SnapshotLog => _snapshotLog;

    [JsonInclude]
    [JsonPropertyName("metadata-log")]
    internal List<MetadataLogEntry> _metadataLog { get; set; } = [];

    public IReadOnlyList<MetadataLogEntry> MetadataLog => _metadataLog;

    [JsonInclude]
    [JsonPropertyName("statistics")]
    internal List<StatisticsFile> _statistics { get; set; } = [];

    public IReadOnlyList<StatisticsFile> Statistics => _statistics;

    [JsonInclude]
    [JsonPropertyName("partition-statistics")]
    internal List<PartitionStatisticsFile> _partitionStatistics { get; set; } = [];

    public IReadOnlyList<PartitionStatisticsFile> PartitionStatistics => _partitionStatistics;

    public IReadOnlyDictionary<long, Snapshot> SnapshotsById => _snapshotsById;
    public IReadOnlyDictionary<int, Schema> SchemasById => _schemasById;

    public void Apply(IEnumerable<ITableUpdate> updates)
    {
        foreach (ITableUpdate update in updates)
            switch (update)
            {
                case AddEncryptionKeyTableUpdate addEncryptionKeyTableUpdate:
                    _encryptionKeys.Add(addEncryptionKeyTableUpdate.EncryptionKey);
                    break;
                case AddPartitionSpecTableUpdate addPartitionSpecTableUpdate:
                    _partitionSpecs.Add(addPartitionSpecTableUpdate.Spec);
                    break;
                case AddSchemaTableUpdate addSchemaTableUpdate:
                    _schemas.Add(addSchemaTableUpdate.Schema);
                    _schemasById[addSchemaTableUpdate.Schema.SchemaId!.Value] = addSchemaTableUpdate.Schema;
                    break;
                case AddSnapshotTableUpdate addSnapshotTableUpdate:
                    _snapshots.Add(addSnapshotTableUpdate.Snapshot);
                    _snapshotLog.Add(
                        new SnapshotLogEntry(
                            addSnapshotTableUpdate.Snapshot.SnapshotId,
                            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
                    _snapshotsById[addSnapshotTableUpdate.Snapshot.SnapshotId] = addSnapshotTableUpdate.Snapshot;
                    break;
                case AddSortOrderTableUpdate addSortOrderTableUpdate:
                    _sortOrders.Add(addSortOrderTableUpdate.SortOrder);
                    break;
                case AddViewVersionTableUpdate:
                    throw new NotImplementedException();
                case AssignUuidTableUpdate:
                    throw new InvalidOperationException("Can't set UUID for existing table");
                case RemoveEncryptionKeyTableUpdate removeEncryptionKeyTableUpdate:
                    _encryptionKeys.RemoveAll(key => key.KeyId == removeEncryptionKeyTableUpdate.KeyId);
                    break;
                case RemovePartitionSpecsTableUpdate removePartitionSpecsTableUpdate:
                    ImmutableHashSet<int> specIds = removePartitionSpecsTableUpdate.SpecIds.ToImmutableHashSet();
                    _partitionSpecs.RemoveAll(spec => spec.SpecId is not null && specIds.Contains((int)spec.SpecId!));
                    break;
                case RemovePartitionStatisticsTableUpdate:
                    throw new NotImplementedException();
                case RemovePropertiesTableUpdate removePropertiesTableUpdate:
                    removePropertiesTableUpdate.Removals.ForEach(removal => _properties.Remove(removal));
                    break;
                case RemoveSchemasTableUpdate removeSchemasTableUpdate:
                    ImmutableHashSet<int> schemaIds = removeSchemasTableUpdate.SchemaIds.ToImmutableHashSet();
                    _schemas.RemoveAll(schema =>
                        schema.SchemaId is not null && schemaIds.Contains((int)schema.SchemaId!));
                    foreach (var schemaId in schemaIds) _schemasById.Remove(schemaId);
                    break;
                case RemoveSnapshotRefTableUpdate removeSnapshotRefTableUpdate:
                    _refs.Remove(removeSnapshotRefTableUpdate.RefName);
                    break;
                case RemoveSnapshotsTableUpdate removeSnapshotsTableUpdate:
                    // TODO do we need to remove the refs?
                    ImmutableHashSet<long> snapshotIds = removeSnapshotsTableUpdate.SnapshotIds.ToImmutableHashSet();
                    _snapshots.RemoveAll(snapshot => snapshotIds.Contains(snapshot.SnapshotId));
                    removeSnapshotsTableUpdate.SnapshotIds.ForEach(id => _snapshotsById.Remove(id));
                    break;
                case RemoveStatisticsTableUpdate:
                    throw new NotImplementedException();
                case SetCurrentSchemaTableUpdate setCurrentSchemaTableUpdate:
                    CurrentSchemaId = setCurrentSchemaTableUpdate.SchemaId;
                    break;
                case SetCurrentViewVersionTableUpdate:
                    throw new NotImplementedException();
                case SetDefaultSortOrderTableUpdate setDefaultSortOrderTableUpdate:
                    DefaultSortOrderId = setDefaultSortOrderTableUpdate.SortOrderId;
                    break;
                case SetDefaultSpecTableUpdate setDefaultSpecTableUpdate:
                    DefaultSpecId = setDefaultSpecTableUpdate.SpecId;
                    break;
                case SetLocationTableUpdate setLocationTableUpdate:
                    if (setLocationTableUpdate.Location != Location)
                        throw new InvalidOperationException("Can't set location for existing table");
                    break;
                case SetPartitionStatisticsTableUpdate:
                    throw new NotImplementedException();
                case SetPropertiesTableUpdate setPropertiesTableUpdate:
                    foreach (KeyValuePair<string, string> kvp in setPropertiesTableUpdate.Updates)
                        _properties.Add(kvp.Key, kvp.Value);
                    break;
                case SetSnapshotRefTableUpdate setSnapshotRefTableUpdate:
                    CurrentSnapshotId = setSnapshotRefTableUpdate.SnapshotId;
                    break;
                case SetStatisticsTableUpdate:
                    throw new NotImplementedException();
                case UpgradeFormatVersionTableUpdate:
                    // TODO this is complicated
                    throw new NotImplementedException();
                default:
                    throw new ArgumentOutOfRangeException(nameof(update));
            }
    }
}