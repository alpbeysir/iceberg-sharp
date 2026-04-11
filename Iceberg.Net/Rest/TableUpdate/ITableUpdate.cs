using System.Text.Json.Serialization;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Rest.TableUpdate;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "action")]
[JsonDerivedType(typeof(AssignUuidTableUpdate), "assign-uuid")]
[JsonDerivedType(typeof(UpgradeFormatVersionTableUpdate), "upgrade-format-version")]
[JsonDerivedType(typeof(AddSchemaTableUpdate), "add-schema")]
[JsonDerivedType(typeof(SetCurrentSchemaTableUpdate), "set-current-schema")]
[JsonDerivedType(typeof(AddPartitionSpecTableUpdate), "add-spec")]
[JsonDerivedType(typeof(SetDefaultSpecTableUpdate), "set-default-spec")]
[JsonDerivedType(typeof(AddSortOrderTableUpdate), "add-sort-order")]
[JsonDerivedType(typeof(SetDefaultSortOrderTableUpdate), "set-default-sort-order")]
[JsonDerivedType(typeof(AddSnapshotTableUpdate), "add-snapshot")]
[JsonDerivedType(typeof(SetSnapshotRefTableUpdate), "set-snapshot-ref")]
[JsonDerivedType(typeof(RemoveSnapshotsTableUpdate), "remove-snapshots")]
[JsonDerivedType(typeof(RemoveSnapshotRefTableUpdate), "remove-snapshot-ref")]
[JsonDerivedType(typeof(SetLocationTableUpdate), "set-location")]
[JsonDerivedType(typeof(SetPropertiesTableUpdate), "set-properties")]
[JsonDerivedType(typeof(RemovePropertiesTableUpdate), "remove-properties")]
[JsonDerivedType(typeof(AddViewVersionTableUpdate), "add-view-version")]
[JsonDerivedType(typeof(SetCurrentViewVersionTableUpdate), "set-current-view-version")]
[JsonDerivedType(typeof(SetStatisticsTableUpdate), "set-statistics")]
[JsonDerivedType(typeof(RemoveStatisticsTableUpdate), "remove-statistics")]
[JsonDerivedType(typeof(SetPartitionStatisticsTableUpdate), "set-partition-statistics")]
[JsonDerivedType(typeof(RemovePartitionStatisticsTableUpdate), "remove-partition-statistics")]
[JsonDerivedType(typeof(RemovePartitionSpecsTableUpdate), "remove-partition-specs")]
[JsonDerivedType(typeof(RemoveSchemasTableUpdate), "remove-schemas")]
[JsonDerivedType(typeof(AddEncryptionKeyTableUpdate), "add-encryption-key")]
[JsonDerivedType(typeof(RemoveEncryptionKeyTableUpdate), "remove-encryption-key")]
public interface ITableUpdate;

[method: JsonConstructor]
public record AddEncryptionKeyTableUpdate(
    [property: JsonPropertyName("encryption-key")]
    EncryptedKey EncryptionKey) : ITableUpdate;

[method: JsonConstructor]
public record AddPartitionSpecTableUpdate(
    [property: JsonPropertyName("spec")] PartitionSpec Spec) : ITableUpdate;

[method: JsonConstructor]
public record AddSnapshotTableUpdate(
    [property: JsonPropertyName("snapshot")]
    Snapshot Snapshot) : ITableUpdate;

[method: JsonConstructor]
public record AddSortOrderTableUpdate(
    [property: JsonPropertyName("sort-order")]
    SortOrder SortOrder) : ITableUpdate;

[method: JsonConstructor]
public record RemoveEncryptionKeyTableUpdate(
    [property: JsonPropertyName("key-id")] string KeyId) : ITableUpdate;

[method: JsonConstructor]
public record RemovePartitionSpecsTableUpdate(
    [property: JsonPropertyName("spec-ids")]
    List<int> SpecIds) : ITableUpdate;

[method: JsonConstructor]
public record RemovePartitionStatisticsTableUpdate(
    [property: JsonPropertyName("snapshot-id")]
    long SnapshotId) : ITableUpdate;

[method: JsonConstructor]
public record RemovePropertiesTableUpdate(
    [property: JsonPropertyName("removals")]
    List<string> Removals) : ITableUpdate;

[method: JsonConstructor]
public record RemoveSchemasTableUpdate(
    [property: JsonPropertyName("schema-ids")]
    List<int> SchemaIds) : ITableUpdate;

[method: JsonConstructor]
public record RemoveSnapshotRefTableUpdate(
    [property: JsonPropertyName("ref-name")]
    string RefName) : ITableUpdate;

[method: JsonConstructor]
public record RemoveSnapshotsTableUpdate(
    [property: JsonPropertyName("snapshot-ids")]
    List<long> SnapshotIds) : ITableUpdate;

[method: JsonConstructor]
public record RemoveStatisticsTableUpdate(
    [property: JsonPropertyName("snapshot-id")]
    long SnapshotId) : ITableUpdate;

[method: JsonConstructor]
public record SetCurrentSchemaTableUpdate(
    /// <summary>
    ///     Schema ID to set as current, or -1 to set last added schema
    /// </summary>
    [property: JsonPropertyName("schema-id")]
    int SchemaId) : ITableUpdate;

[method: JsonConstructor]
public record SetCurrentViewVersionTableUpdate(
    /// <summary>
    ///     The view version id to set as current, or -1 to set last added view version id
    /// </summary>
    [property: JsonPropertyName("view-version-id")]
    int ViewVersionId) : ITableUpdate;

[method: JsonConstructor]
public record SetDefaultSortOrderTableUpdate(
    /// <summary>
    ///     Sort order ID to set as the default, or -1 to set last added sort order
    /// </summary>
    [property: JsonPropertyName("sort-order-id")]
    int SortOrderId) : ITableUpdate;

[method: JsonConstructor]
public record SetDefaultSpecTableUpdate(
    /// <summary>
    ///     Partition spec ID to set as the default, or -1 to set last added spec
    /// </summary>
    [property: JsonPropertyName("spec-id")]
    int SpecId) : ITableUpdate;

[method: JsonConstructor]
public record SetLocationTableUpdate(
    [property: JsonPropertyName("location")]
    string Location) : ITableUpdate;

[method: JsonConstructor]
public record SetPartitionStatisticsTableUpdate(
    [property: JsonPropertyName("partition-statistics")]
    PartitionStatisticsFile PartitionStatistics) : ITableUpdate;

[method: JsonConstructor]
public record SetPropertiesTableUpdate(
    [property: JsonPropertyName("updates")]
    IDictionary<string, string> Updates) : ITableUpdate;

[method: JsonConstructor]
public record SetSnapshotRefTableUpdate(
    [property: JsonPropertyName("ref-name")]
    string RefName,
    [property: JsonPropertyName("type")]
    [property: JsonConverter(typeof(JsonStringEnumConverter<SnapshotReferenceType>))]
    SnapshotReferenceType Type,
    [property: JsonPropertyName("snapshot-id")]
    long SnapshotId,
    [property: JsonPropertyName("max-ref-age-ms")]
    long? MaxRefAgeMs,
    [property: JsonPropertyName("max-snapshot-age-ms")]
    long? MaxSnapshotAgeMs,
    [property: JsonPropertyName("min-snapshots-to-keep")]
    int? MinSnapshotsToKeep) : ITableUpdate;

[method: JsonConstructor]
public record SetStatisticsTableUpdate(
    [property: JsonPropertyName("snapshot-id")]
    [property: Obsolete]
    long? SnapshotId,
    [property: JsonPropertyName("statistics")]
    StatisticsFile Statistics) : ITableUpdate;

[method: JsonConstructor]
public record UpgradeFormatVersionTableUpdate(
    [property: JsonPropertyName("format-version")]
    int FormatVersion) : ITableUpdate;

[method: JsonConstructor]
public record AddSchemaTableUpdate(
    [property: JsonPropertyName("schema")] Schemas.Schema Schema,
    [property: JsonPropertyName("last-column-id")]
    [property: Obsolete]
    int? LastColumnId = null) : ITableUpdate;

/// <summary>
///     Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the
///     UUID if a table/view already has a UUID assigned
/// </summary>
[method: JsonConstructor]
public record AssignUuidTableUpdate(
    [property: JsonPropertyName("uuid")] string Uuid) : ITableUpdate;

[method: JsonConstructor]
public record AddViewVersionTableUpdate(
    [property: JsonPropertyName("view-version")]
    ViewVersion ViewVersion) : ITableUpdate;