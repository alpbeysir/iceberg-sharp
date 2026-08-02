// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Avro schema type tags per the Avro specification.
/// </summary>
public enum AvroType
{
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record,
    Enum,
    Array,
    Map,
    Fixed,
    Union,
}

/// <summary>
/// Base class for the Avro schema tree. Each node represents a type in the schema.
/// </summary>
public abstract class AvroSchemaNode
{
    public abstract AvroType Type { get; }

    /// <summary>Optional logical type annotation (e.g. "decimal", "date", "timestamp-millis").</summary>
    public string? LogicalType { get; init; }

    /// <summary>Format-specific properties not defined by the Avro specification.</summary>
    public IReadOnlyDictionary<string, AvroValue> CustomProperties { get; init; } =
        new Dictionary<string, AvroValue>();
}

public sealed class AvroPrimitiveSchema : AvroSchemaNode
{
    public override AvroType Type { get; }

    /// <summary>Decimal precision (total digits). Only meaningful when LogicalType is "decimal".</summary>
    public int? Precision { get; init; }

    /// <summary>Decimal scale (digits after decimal point). Only meaningful when LogicalType is "decimal".</summary>
    public int? Scale { get; init; }

    public AvroPrimitiveSchema(AvroType type)
    {
        Type = type;
    }

    // Singletons for the primitive types (no logical type)
    public static readonly AvroPrimitiveSchema Null = new(AvroType.Null);
    public static readonly AvroPrimitiveSchema Boolean = new(AvroType.Boolean);
    public static readonly AvroPrimitiveSchema Int = new(AvroType.Int);
    public static readonly AvroPrimitiveSchema Long = new(AvroType.Long);
    public static readonly AvroPrimitiveSchema Float = new(AvroType.Float);
    public static readonly AvroPrimitiveSchema Double = new(AvroType.Double);
    public static readonly AvroPrimitiveSchema Bytes = new(AvroType.Bytes);
    public static readonly AvroPrimitiveSchema String = new(AvroType.String);
}

public sealed class AvroRecordSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Record;
    public string Name { get; }
    public string? Namespace { get; }
    public string FullName => Namespace != null ? $"{Namespace}.{Name}" : Name;
    public string? Doc { get; init; }
    public IReadOnlyList<string> Aliases { get; init; } = [];
    public IReadOnlyList<AvroFieldNode> Fields { get; }

    public AvroRecordSchema(string name, string? ns, IReadOnlyList<AvroFieldNode> fields)
    {
        Name = name;
        Namespace = ns;
        Fields = fields;
    }
}

public sealed class AvroFieldNode
{
    public string Name { get; }
    public AvroSchemaNode Schema { get; }
    public IReadOnlyList<string> Aliases { get; init; } = [];
    public string? Doc { get; init; }

    /// <summary>Format-specific field properties, such as Iceberg's <c>field-id</c>.</summary>
    public IReadOnlyDictionary<string, AvroValue> CustomProperties { get; init; } =
        new Dictionary<string, AvroValue>();

    /// <summary>
    /// The default value, or null if no default is declared. Use <see cref="AvroValue.Null"/>
    /// for an explicit Avro null default.
    /// Avro spec: fields with no default are required.
    /// </summary>
    public AvroValue? Default { get; init; }

    public AvroFieldNode(string name, AvroSchemaNode schema)
    {
        Name = name;
        Schema = schema;
    }
}

public sealed class AvroEnumSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Enum;
    public string Name { get; }
    public string? Namespace { get; }
    public string FullName => Namespace != null ? $"{Namespace}.{Name}" : Name;
    public IReadOnlyList<string> Symbols { get; }
    public string? Default { get; init; }
    public string? Doc { get; init; }
    public IReadOnlyList<string> Aliases { get; init; } = [];

    public AvroEnumSchema(string name, string? ns, IReadOnlyList<string> symbols)
    {
        Name = name;
        Namespace = ns;
        Symbols = symbols;
    }
}

public sealed class AvroArraySchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Array;
    public AvroSchemaNode Items { get; }

    public AvroArraySchema(AvroSchemaNode items)
    {
        Items = items;
    }
}

public sealed class AvroMapSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Map;
    public AvroSchemaNode Values { get; }

    public AvroMapSchema(AvroSchemaNode values)
    {
        Values = values;
    }
}

public sealed class AvroFixedSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Fixed;
    public string Name { get; }
    public string? Namespace { get; }
    public string FullName => Namespace != null ? $"{Namespace}.{Name}" : Name;
    public int Size { get; }
    public IReadOnlyList<string> Aliases { get; init; } = [];

    /// <summary>Decimal precision (total digits). Only meaningful when LogicalType is "decimal".</summary>
    public int? Precision { get; init; }

    /// <summary>Decimal scale (digits after decimal point). Only meaningful when LogicalType is "decimal".</summary>
    public int? Scale { get; init; }

    public AvroFixedSchema(string name, string? ns, int size)
    {
        Name = name;
        Namespace = ns;
        Size = size;
    }
}

public sealed class AvroUnionSchema : AvroSchemaNode
{
    public override AvroType Type => AvroType.Union;
    public IReadOnlyList<AvroSchemaNode> Branches { get; }

    public AvroUnionSchema(IReadOnlyList<AvroSchemaNode> branches)
    {
        Branches = branches;
    }

    /// <summary>
    /// Returns true if this is a nullable union (exactly 2 branches, one of which is null).
    /// </summary>
    public bool IsNullable(out AvroSchemaNode innerType, out int nullIndex)
    {
        if (Branches.Count == 2)
        {
            if (Branches[0].Type == AvroType.Null)
            {
                innerType = Branches[1];
                nullIndex = 0;
                return true;
            }
            if (Branches[1].Type == AvroType.Null)
            {
                innerType = Branches[0];
                nullIndex = 1;
                return true;
            }
        }
        innerType = AvroPrimitiveSchema.Null;
        nullIndex = -1;
        return false;
    }
}
