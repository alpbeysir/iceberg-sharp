// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;

namespace EngineeredWood.Avro.Schema;

/// <summary>A typed JSON-compatible value used by Avro defaults and custom properties.</summary>
public readonly struct AvroValue
{
    private readonly object? _value;

    private AvroValue(AvroValueKind kind, object? value)
    {
        Kind = kind;
        _value = value;
    }

    public AvroValueKind Kind { get; }

    public static AvroValue Null { get; } = new(AvroValueKind.Null, null);

    public static AvroValue Of(bool value) => new(AvroValueKind.Boolean, value);
    public static AvroValue Of(int value) => new(AvroValueKind.Integer, (long)value);
    public static AvroValue Of(long value) => new(AvroValueKind.Integer, value);
    public static AvroValue Of(float value) => Number(value.ToString("R", CultureInfo.InvariantCulture));
    public static AvroValue Of(double value) => Number(value.ToString("R", CultureInfo.InvariantCulture));
    public static AvroValue Of(decimal value) => Number(value.ToString(CultureInfo.InvariantCulture));
    public static AvroValue Of(string value) =>
        new(AvroValueKind.String, value ?? throw new ArgumentNullException(nameof(value)));
    public static AvroValue Of(IReadOnlyList<AvroValue> values) =>
        new(AvroValueKind.Array, values ?? throw new ArgumentNullException(nameof(values)));
    public static AvroValue Of(IReadOnlyDictionary<string, AvroValue> values) =>
        new(AvroValueKind.Object, values ?? throw new ArgumentNullException(nameof(values)));

    public static implicit operator AvroValue(bool value) => Of(value);
    public static implicit operator AvroValue(int value) => Of(value);
    public static implicit operator AvroValue(long value) => Of(value);
    public static implicit operator AvroValue(float value) => Of(value);
    public static implicit operator AvroValue(double value) => Of(value);
    public static implicit operator AvroValue(decimal value) => Of(value);
    public static implicit operator AvroValue(string value) => Of(value);

    public bool AsBoolean => Kind == AvroValueKind.Boolean
        ? (bool)_value!
        : throw InvalidAccess(AvroValueKind.Boolean);

    public int AsInt32 => checked((int)AsInt64);

    public long AsInt64 => Kind == AvroValueKind.Integer
        ? (long)_value!
        : throw InvalidAccess(AvroValueKind.Integer);

    public float AsSingle => float.Parse(AsNumberText, NumberStyles.Float, CultureInfo.InvariantCulture);

    public double AsDouble => double.Parse(AsNumberText, NumberStyles.Float, CultureInfo.InvariantCulture);

    public string AsString => Kind == AvroValueKind.String
        ? (string)_value!
        : throw InvalidAccess(AvroValueKind.String);

    public IReadOnlyList<AvroValue> AsArray => Kind == AvroValueKind.Array
        ? (IReadOnlyList<AvroValue>)_value!
        : throw InvalidAccess(AvroValueKind.Array);

    public IReadOnlyDictionary<string, AvroValue> AsObject => Kind == AvroValueKind.Object
        ? (IReadOnlyDictionary<string, AvroValue>)_value!
        : throw InvalidAccess(AvroValueKind.Object);

    internal static AvroValue FromJsonElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Null => Null,
            JsonValueKind.True => Of(true),
            JsonValueKind.False => Of(false),
            JsonValueKind.String => Of(element.GetString()!),
            JsonValueKind.Number when element.TryGetInt64(out long integer) => Of(integer),
            JsonValueKind.Number => new AvroValue(AvroValueKind.Number, element.GetRawText()),
            JsonValueKind.Array => Of(element.EnumerateArray().Select(FromJsonElement).ToArray()),
            JsonValueKind.Object => Of(element.EnumerateObject().ToDictionary(
                property => property.Name,
                property => FromJsonElement(property.Value),
                StringComparer.Ordinal)),
            _ => throw new InvalidOperationException($"Unsupported JSON value kind {element.ValueKind}.")
        };
    }

    internal void WriteTo(Utf8JsonWriter writer)
    {
        switch (Kind)
        {
            case AvroValueKind.Null:
                writer.WriteNullValue();
                break;
            case AvroValueKind.Boolean:
                writer.WriteBooleanValue(AsBoolean);
                break;
            case AvroValueKind.Integer:
                writer.WriteNumberValue(AsInt64);
                break;
            case AvroValueKind.Number:
                writer.WriteRawValue((string)_value!);
                break;
            case AvroValueKind.String:
                writer.WriteStringValue(AsString);
                break;
            case AvroValueKind.Array:
                writer.WriteStartArray();
                foreach (AvroValue value in AsArray) value.WriteTo(writer);
                writer.WriteEndArray();
                break;
            case AvroValueKind.Object:
                writer.WriteStartObject();
                foreach ((string name, AvroValue value) in AsObject)
                {
                    writer.WritePropertyName(name);
                    value.WriteTo(writer);
                }
                writer.WriteEndObject();
                break;
            default:
                throw new InvalidOperationException($"Unknown Avro value kind {Kind}.");
        }
    }

    private string AsNumberText => Kind switch
    {
        AvroValueKind.Integer => AsInt64.ToString(CultureInfo.InvariantCulture),
        AvroValueKind.Number => (string)_value!,
        _ => throw InvalidAccess(AvroValueKind.Number)
    };

    private static AvroValue Number(string value)
    {
        if (!double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double number) ||
            !double.IsFinite(number))
            throw new ArgumentOutOfRangeException(nameof(value), "Avro numeric values must be finite JSON numbers.");
        return new AvroValue(AvroValueKind.Number, value);
    }

    private InvalidOperationException InvalidAccess(AvroValueKind expected) =>
        new($"Cannot read an Avro {Kind} value as {expected}.");
}

public enum AvroValueKind
{
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Array,
    Object
}
