using System.Text.Json.Serialization;
using Iceberg.Net.Metadata;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Serialization;

[JsonSerializable(typeof(PrimitiveType))]
[JsonSerializable(typeof(StructType))]
[JsonSerializable(typeof(StructField))]
[JsonSerializable(typeof(ListType))]
[JsonSerializable(typeof(MapType))]
[JsonSerializable(typeof(Schemas.Schema))]
[JsonSerializable(typeof(List<PartitionField>))]
[JsonSerializable(typeof(ITableUpdate))]
[JsonSerializable(typeof(ITableRequirement))]
[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal partial class IcebergJsonContext : JsonSerializerContext;
