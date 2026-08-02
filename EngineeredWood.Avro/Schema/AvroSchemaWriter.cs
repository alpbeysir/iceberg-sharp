// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Serializes an <see cref="AvroSchemaNode"/> tree back to JSON.
/// </summary>
internal static class AvroSchemaWriter
{
    public static string ToJson(AvroSchemaNode node)
    {
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms))
        {
            var namedTypes = new HashSet<string>();
            WriteNode(writer, node, namedTypes);
        }
        return System.Text.Encoding.UTF8.GetString(ms.ToArray());
    }

    private static void WriteNode(Utf8JsonWriter writer, AvroSchemaNode node, HashSet<string> namedTypes)
    {
        switch (node)
        {
            case AvroPrimitiveSchema p:
                WritePrimitive(writer, p);
                break;
            case AvroRecordSchema r:
                WriteRecord(writer, r, namedTypes);
                break;
            case AvroEnumSchema e:
                WriteEnum(writer, e, namedTypes);
                break;
            case AvroArraySchema a:
                writer.WriteStartObject();
                writer.WriteString("type", "array");
                writer.WritePropertyName("items");
                WriteNode(writer, a.Items, namedTypes);
                WriteLogicalType(writer, a.LogicalType);
                WriteCustomProperties(writer, a.CustomProperties);
                writer.WriteEndObject();
                break;
            case AvroMapSchema m:
                writer.WriteStartObject();
                writer.WriteString("type", "map");
                writer.WritePropertyName("values");
                WriteNode(writer, m.Values, namedTypes);
                WriteLogicalType(writer, m.LogicalType);
                WriteCustomProperties(writer, m.CustomProperties);
                writer.WriteEndObject();
                break;
            case AvroFixedSchema f:
                WriteFixed(writer, f, namedTypes);
                break;
            case AvroUnionSchema u:
                writer.WriteStartArray();
                foreach (var branch in u.Branches)
                    WriteNode(writer, branch, namedTypes);
                writer.WriteEndArray();
                break;
        }
    }

    private static void WritePrimitive(Utf8JsonWriter writer, AvroPrimitiveSchema p)
    {
        if (p.LogicalType != null || p.CustomProperties.Count != 0)
        {
            writer.WriteStartObject();
            writer.WriteString("type", PrimitiveTypeName(p.Type));
            WriteLogicalType(writer, p.LogicalType);
            if (p.Precision.HasValue)
                writer.WriteNumber("precision", p.Precision.Value);
            if (p.Scale.HasValue)
                writer.WriteNumber("scale", p.Scale.Value);
            WriteCustomProperties(writer, p.CustomProperties);
            writer.WriteEndObject();
        }
        else
        {
            writer.WriteStringValue(PrimitiveTypeName(p.Type));
        }
    }

    private static void WriteRecord(Utf8JsonWriter writer, AvroRecordSchema r, HashSet<string> namedTypes)
    {
        if (!namedTypes.Add(r.FullName))
        {
            // Already defined — emit as a reference
            writer.WriteStringValue(r.FullName);
            return;
        }

        writer.WriteStartObject();
        writer.WriteString("type", "record");
        writer.WriteString("name", r.Name);
        if (r.Namespace != null)
            writer.WriteString("namespace", r.Namespace);
        if (r.Doc is not null)
            writer.WriteString("doc", r.Doc);
        WriteAliases(writer, r.Aliases);
        WriteLogicalType(writer, r.LogicalType);

        writer.WriteStartArray("fields");
        foreach (var field in r.Fields)
        {
            writer.WriteStartObject();
            writer.WriteString("name", field.Name);
            writer.WritePropertyName("type");
            WriteNode(writer, field.Schema, namedTypes);
            if (field.Default.HasValue)
            {
                writer.WritePropertyName("default");
                field.Default.Value.WriteTo(writer);
            }
            if (field.Doc is not null)
                writer.WriteString("doc", field.Doc);
            WriteAliases(writer, field.Aliases);
            WriteCustomProperties(writer, field.CustomProperties);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
        WriteCustomProperties(writer, r.CustomProperties);
        writer.WriteEndObject();
    }

    private static void WriteEnum(Utf8JsonWriter writer, AvroEnumSchema e, HashSet<string> namedTypes)
    {
        if (!namedTypes.Add(e.FullName))
        {
            writer.WriteStringValue(e.FullName);
            return;
        }

        writer.WriteStartObject();
        writer.WriteString("type", "enum");
        writer.WriteString("name", e.Name);
        if (e.Namespace != null)
            writer.WriteString("namespace", e.Namespace);
        writer.WriteStartArray("symbols");
        foreach (var sym in e.Symbols)
            writer.WriteStringValue(sym);
        writer.WriteEndArray();
        if (e.Default != null)
            writer.WriteString("default", e.Default);
        if (e.Doc is not null)
            writer.WriteString("doc", e.Doc);
        WriteAliases(writer, e.Aliases);
        WriteCustomProperties(writer, e.CustomProperties);
        writer.WriteEndObject();
    }

    private static void WriteFixed(Utf8JsonWriter writer, AvroFixedSchema f, HashSet<string> namedTypes)
    {
        if (!namedTypes.Add(f.FullName))
        {
            writer.WriteStringValue(f.FullName);
            return;
        }

        writer.WriteStartObject();
        writer.WriteString("type", "fixed");
        writer.WriteString("name", f.Name);
        if (f.Namespace != null)
            writer.WriteString("namespace", f.Namespace);
        writer.WriteNumber("size", f.Size);
        if (f.LogicalType != null)
        {
            writer.WriteString("logicalType", f.LogicalType);
            if (f.Precision.HasValue)
                writer.WriteNumber("precision", f.Precision.Value);
            if (f.Scale.HasValue)
                writer.WriteNumber("scale", f.Scale.Value);
        }
        WriteAliases(writer, f.Aliases);
        WriteCustomProperties(writer, f.CustomProperties);
        writer.WriteEndObject();
    }

    private static void WriteAliases(Utf8JsonWriter writer, IReadOnlyList<string> aliases)
    {
        if (aliases.Count == 0) return;
        writer.WriteStartArray("aliases");
        foreach (string alias in aliases) writer.WriteStringValue(alias);
        writer.WriteEndArray();
    }

    private static void WriteLogicalType(Utf8JsonWriter writer, string? logicalType)
    {
        if (logicalType is not null) writer.WriteString("logicalType", logicalType);
    }

    private static void WriteCustomProperties(
        Utf8JsonWriter writer,
        IReadOnlyDictionary<string, AvroValue> properties)
    {
        foreach ((string name, AvroValue value) in properties)
        {
            writer.WritePropertyName(name);
            value.WriteTo(writer);
        }
    }

    private static string PrimitiveTypeName(AvroType type) => type switch
    {
        AvroType.Null => "null",
        AvroType.Boolean => "boolean",
        AvroType.Int => "int",
        AvroType.Long => "long",
        AvroType.Float => "float",
        AvroType.Double => "double",
        AvroType.Bytes => "bytes",
        AvroType.String => "string",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };
}
