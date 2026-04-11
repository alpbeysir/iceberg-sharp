using System.Text.Json.Serialization;
using Iceberg.Net.Rest.Expression;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Rest;

[JsonSerializable(typeof(PrimitiveType))]
[JsonSerializable(typeof(StructType))]
[JsonSerializable(typeof(StructField))]
[JsonSerializable(typeof(ListType))]
[JsonSerializable(typeof(MapType))]
[JsonSerializable(typeof(Schemas.Schema))]
[JsonSerializable(typeof(CreateNamespaceRequest))]
[JsonSerializable(typeof(CreateNamespaceResponse))]
[JsonSerializable(typeof(CreateTableRequest))]
[JsonSerializable(typeof(ListTablesResponse))]
[JsonSerializable(typeof(GetNamespaceResponse))]
[JsonSerializable(typeof(CatalogConfig))]
[JsonSerializable(typeof(IcebergErrorResponse))]
[JsonSerializable(typeof(LoadTableResult))]
[JsonSerializable(typeof(ListNamespacesResponse))]
[JsonSerializable(typeof(CommitTableRequest))]
[JsonSerializable(typeof(CommitTableResponse))]
[JsonSerializable(typeof(ITableUpdate))]
[JsonSerializable(typeof(ITableRequirement))]
[JsonSerializable(typeof(IExpression))]
[JsonSerializable(typeof(TransformTerm))]
[JsonSerializable(typeof(FieldMapping))]
[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
public partial class SourceGenerationContext : JsonSerializerContext;