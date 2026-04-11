using System.Text.Json;
using System.Text.Json.Serialization;
using Iceberg.Net.Rest;

namespace Iceberg.Net.Schemas;

[JsonConverter(typeof(IcebergTypeConverter))]
public interface IIcebergType;

// --- Complex Types ---
public record StructType(
    [property: JsonPropertyName("fields")] List<StructField> Fields
) : IIcebergType
{
    [property: JsonPropertyName("type")] public string Type => "struct";
}

public record ListType(
    [property: JsonPropertyName("element-id")]
    int ElementId,
    [property: JsonPropertyName("element")]
    IIcebergType Element,
    [property: JsonPropertyName("element-required")]
    bool ElementRequired
) : IIcebergType
{
    [property: JsonPropertyName("type")] public string Type => "list";
}

public record MapType(
    [property: JsonPropertyName("key-id")] int KeyId,
    [property: JsonPropertyName("key")] IIcebergType Key,
    [property: JsonPropertyName("value-id")]
    int ValueId,
    [property: JsonPropertyName("value")] IIcebergType Value,
    [property: JsonPropertyName("value-required")]
    bool ValueRequired
) : IIcebergType
{
    [property: JsonPropertyName("type")] public string Type => "map";
}

public record StructField(
    [property: JsonPropertyName("id")] int Id,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("type")] IIcebergType FieldType,
    [property: JsonPropertyName("required")]
    bool Required,
    [property: JsonPropertyName("doc")] string? Doc = null,
    [property: JsonPropertyName("initial-default")]
    object? InitialDefault = null,
    [property: JsonPropertyName("write-default")]
    object? WriteDefault = null
);

public record Schema(
    List<StructField> Fields,
    [property: JsonPropertyName("schema-id")]
    int? SchemaId = null,
    [property: JsonPropertyName("identifier-field-ids")]
    List<int>? IdentifierFieldIds = null
) : StructType(Fields);

public class IcebergTypeConverter : JsonConverter<IIcebergType>
{
    public override IIcebergType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String) return new PrimitiveType(reader.GetString()!);

        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        var typeKind = root.GetProperty("type").GetString();

        return typeKind switch
        {
            "struct" => JsonSerializer.Deserialize<StructType>(
                root.GetRawText(),
                SourceGenerationContext.Default.StructType)!,
            "list" => JsonSerializer.Deserialize<ListType>(
                root.GetRawText(),
                SourceGenerationContext.Default.ListType)!,
            "map" => JsonSerializer.Deserialize<MapType>(root.GetRawText(), SourceGenerationContext.Default.MapType)!,
            _ => throw new JsonException($"Unknown type kind: {typeKind}")
        };
    }

    public override void Write(Utf8JsonWriter writer, IIcebergType value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case ListType:
                JsonSerializer.Serialize(writer, value, SourceGenerationContext.Default.ListType);
                break;
            case MapType:
                JsonSerializer.Serialize(writer, value, SourceGenerationContext.Default.MapType);
                break;
            case PrimitiveType primitiveType:
                writer.WriteStringValue(primitiveType.Name);
                break;
            case Schema:
                JsonSerializer.Serialize(writer, value, SourceGenerationContext.Default.Schema);
                break;
            case StructType:
                JsonSerializer.Serialize(writer, value, SourceGenerationContext.Default.StructType);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(value));
        }
    }
}