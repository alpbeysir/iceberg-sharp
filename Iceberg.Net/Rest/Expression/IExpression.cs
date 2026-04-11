using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest.Expression;

public enum UnaryOperator
{
    [JsonStringEnumMemberName("is-null")] IsNull,
    [JsonStringEnumMemberName("not-null")] NotNull,
    [JsonStringEnumMemberName("is-nan")] IsNan,
    [JsonStringEnumMemberName("not-nan")] NotNan
}

public enum LiteralOperator
{
    [JsonStringEnumMemberName("lt")] Lt,
    [JsonStringEnumMemberName("lt-eq")] LtEq,
    [JsonStringEnumMemberName("gt")] Gt,
    [JsonStringEnumMemberName("gt-eq")] GtEq,
    [JsonStringEnumMemberName("eq")] Eq,
    [JsonStringEnumMemberName("not-eq")] NotEq,

    [JsonStringEnumMemberName("starts-with")]
    StartsWith,

    [JsonStringEnumMemberName("not-starts-with")]
    NotStartsWith
}

public enum AndOrOperator
{
    [JsonStringEnumMemberName("and")] And,
    [JsonStringEnumMemberName("or")] Or
}

public enum SetOperator
{
    [JsonStringEnumMemberName("in")] In,
    [JsonStringEnumMemberName("not-in")] NotIn
}

// [JsonConverter(typeof(ExpressionConverter))]
[JsonDerivedType(typeof(TrueExpression), "true")]
[JsonDerivedType(typeof(FalseExpression), "false")]
[JsonDerivedType(typeof(AndOrExpression), "and")]
[JsonDerivedType(typeof(AndOrExpression), "or")]
[JsonDerivedType(typeof(NotExpression), "not")]
[JsonDerivedType(typeof(SetExpression), "in")]
[JsonDerivedType(typeof(SetExpression), "not-in")]
[JsonDerivedType(typeof(LiteralExpression), "lt")]
[JsonDerivedType(typeof(LiteralExpression), "lt-eq")]
[JsonDerivedType(typeof(LiteralExpression), "gt")]
[JsonDerivedType(typeof(LiteralExpression), "gt-eq")]
[JsonDerivedType(typeof(LiteralExpression), "eq")]
[JsonDerivedType(typeof(LiteralExpression), "not-eq")]
[JsonDerivedType(typeof(LiteralExpression), "starts-with")]
[JsonDerivedType(typeof(LiteralExpression), "not-starts-with")]
[JsonDerivedType(typeof(UnaryExpression), "is-null")]
[JsonDerivedType(typeof(UnaryExpression), "not-null")]
[JsonDerivedType(typeof(UnaryExpression), "is-nan")]
[JsonDerivedType(typeof(UnaryExpression), "not-nan")]
public interface IExpression;

public record TrueExpression : IExpression;

public record FalseExpression : IExpression;

public record AndOrExpression(
    [property: JsonPropertyName("type")] AndOrOperator Operator,
    [property: JsonPropertyName("left")] IExpression Left,
    [property: JsonPropertyName("right")] IExpression Right
) : IExpression;

public record NotExpression(
    [property: JsonPropertyName("child")] IExpression Child
) : IExpression;

public record UnaryExpression(
    [property: JsonPropertyName("type")] UnaryOperator Operator,
    [property: JsonPropertyName("term")] ITerm Term
) : IExpression;

public record LiteralExpression(
    [property: JsonPropertyName("type")] LiteralOperator Operator,
    [property: JsonPropertyName("term")] ITerm Term,
    [property: JsonPropertyName("value")] IPrimitiveTypeValue Value
) : IExpression;

public record SetExpression(
    [property: JsonPropertyName("type")] SetOperator Operator,
    [property: JsonPropertyName("term")] ITerm Term,
    [property: JsonPropertyName("values")] List<IPrimitiveTypeValue> Values
) : IExpression;

[JsonDerivedType(typeof(BooleanValue))]
[JsonDerivedType(typeof(IntegerValue))]
[JsonDerivedType(typeof(LongValue))]
[JsonDerivedType(typeof(FloatValue))]
[JsonDerivedType(typeof(DoubleValue))]
[JsonDerivedType(typeof(StringValue))]
[JsonDerivedType(typeof(DecimalValue))]
[JsonDerivedType(typeof(UuidValue))]
[JsonDerivedType(typeof(DateValue))]
[JsonDerivedType(typeof(TimeValue))]
[JsonDerivedType(typeof(TimestampValue))]
[JsonDerivedType(typeof(BinaryValue))]
public interface IPrimitiveTypeValue;

public record BooleanValue([property: JsonPropertyName("value")] bool Value) : IPrimitiveTypeValue;

public record IntegerValue([property: JsonPropertyName("value")] int Value) : IPrimitiveTypeValue;

public record LongValue([property: JsonPropertyName("value")] long Value) : IPrimitiveTypeValue;

public record FloatValue([property: JsonPropertyName("value")] float Value) : IPrimitiveTypeValue;

public record DoubleValue([property: JsonPropertyName("value")] double Value) : IPrimitiveTypeValue;

public record StringValue([property: JsonPropertyName("value")] string Value) : IPrimitiveTypeValue;

public record UuidValue([property: JsonPropertyName("value")] Guid Value) : IPrimitiveTypeValue;

// Specialized formats handled as strings per schema
public record DecimalValue([property: JsonPropertyName("value")] string Value) : IPrimitiveTypeValue;

public record DateValue([property: JsonPropertyName("value")] string Value) : IPrimitiveTypeValue;

public record TimeValue([property: JsonPropertyName("value")] string Value) : IPrimitiveTypeValue;

public record TimestampValue([property: JsonPropertyName("value")] string Value) : IPrimitiveTypeValue;

public record BinaryValue([property: JsonPropertyName("value")] string Value) : IPrimitiveTypeValue;

[JsonConverter(typeof(TermConverter))]
public interface ITerm;

// Reference is a simple string in the schema
public record ReferenceTerm(
    string Name
) : ITerm;

public record TransformTerm(
    [property: JsonPropertyName("transform")]
    string Transform,
    [property: JsonPropertyName("term")] string Term,
    [property: JsonPropertyName("type")] string Type = "transform"
) : ITerm;

// internal class ExpressionConverter : JsonConverter<IExpression>
// {
//     public override IExpression? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
//     {
//         using var jsonDoc = JsonDocument.ParseValue(ref reader);
//         var root = jsonDoc.RootElement;
//
//         if (!root.TryGetProperty("type", out var typeProp))
//             throw new JsonException("Expression missing required 'type' field.");
//
//         var typeValue = typeProp.GetString();
//
//         return typeValue switch
//         {
//             "true" => new TrueExpression(),
//             "false" => new FalseExpression(),
//             "and" or "or" => JsonSerializer.Deserialize<AndOrExpression>(
//                 root.GetRawText(),
//                 SourceGenerationContext.Default.AndOrExpression),
//             "not" => JsonSerializer.Deserialize<NotExpression>(
//                 root.GetRawText(),
//                 SourceGenerationContext.Default.NotExpression),
//             "in" or "not-in" => JsonSerializer.Deserialize<SetExpression>(
//                 root.GetRawText(),
//                 SourceGenerationContext.Default.SetExpression),
//             "is-null" or "not-null" or "is-nan" or "not-nan" => JsonSerializer.Deserialize<UnaryExpression>(
//                 root.GetRawText(),
//                 SourceGenerationContext.Default.UnaryExpression),
//             _ => JsonSerializer.Deserialize<LiteralExpression>(
//                 root.GetRawText(),
//                 SourceGenerationContext.Default.LiteralExpression)
//         };
//     }
//
//     public override void Write(Utf8JsonWriter writer, IExpression value, JsonSerializerOptions options)
//     {
//         JsonSerializer.Serialize(writer, value, SourceGenerationContext.Default.IExpression);
//     }
// }

internal class TermConverter : JsonConverter<ITerm>
{
    public override ITerm? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String) return new ReferenceTerm(reader.GetString()!);

        using var jsonDoc = JsonDocument.ParseValue(ref reader);
        return JsonSerializer.Deserialize<TransformTerm>(
            jsonDoc.RootElement.GetRawText(),
            SourceGenerationContext.Default.TransformTerm);
    }

    public override void Write(Utf8JsonWriter writer, ITerm value, JsonSerializerOptions options)
    {
        if (value is ReferenceTerm reference)
            writer.WriteStringValue(reference.Name);
        else
            JsonSerializer.Serialize(writer, value, SourceGenerationContext.Default.ITerm);
    }
}