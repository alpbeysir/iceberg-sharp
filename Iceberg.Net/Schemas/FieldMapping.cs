using System.Text.Json.Serialization;

namespace Iceberg.Net.Schemas;

public record FieldMapping(
    [property: JsonPropertyName("field-id")]
    int FieldId,
    [property: JsonPropertyName("names")] List<string> Names,
    [property: JsonPropertyName("fields")] List<FieldMapping>? Fields = null
)
{
    public const string TableMetadataNameMappingKey = "schema.name-mapping.default";

    public static List<FieldMapping>? FromType(IIcebergType type)
    {
        return type switch
        {
            ListType listType =>
            [
                new FieldMapping(listType.ElementId, ["element"], FromType(listType.Element))
            ],
            MapType mapType =>
            [
                new FieldMapping(mapType.KeyId, ["key"], FromType(mapType.Key)),
                new FieldMapping(mapType.ValueId, ["value"], FromType(mapType.Value))
            ],
            PrimitiveType => null,
            Schema schema => FromFields(schema.Fields),
            StructType structType => FromFields(structType.Fields),
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };
    }

    private static List<FieldMapping> FromFields(List<StructField> fields)
    {
        return fields
            .Select(field => new FieldMapping(field.Id, [field.Name], FromType(field.FieldType)!))
            .ToList();
    }
}