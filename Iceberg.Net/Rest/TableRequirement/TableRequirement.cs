using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.TableRequirement;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(AssertCreate), "assert-create")]
[JsonDerivedType(typeof(AssertTableUuid), "assert-table-uuid")]
[JsonDerivedType(typeof(AssertRefSnapshotId), "assert-ref-snapshot-id")]
[JsonDerivedType(typeof(AssertLastAssignedFieldId), "assert-last-assigned-field-id")]
[JsonDerivedType(typeof(AssertCurrentSchemaId), "assert-current-schema-id")]
[JsonDerivedType(typeof(AssertLastAssignedPartitionId), "assert-last-assigned-partition-id")]
[JsonDerivedType(typeof(AssertDefaultSpecId), "assert-default-spec-id")]
[JsonDerivedType(typeof(AssertDefaultSortOrderId), "assert-default-sort-order-id")]
public interface ITableRequirement;