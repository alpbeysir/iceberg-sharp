using System.Collections.Immutable;
using Iceberg.Net.Misc;
using ParquetSharp;
using ParquetSharp.Schema;

namespace Iceberg.Net.Schemas;

public static class ParquetSharpSchema
{
    public static ParquetIcebergMetadata GetIcebergMetadata(
        SchemaDescriptor parquetSchema,
        Schema iceberg)
    {
        Dictionary<int, PrimitiveType> icebergTypeLookup = [];
        iceberg.Visit((type, _, id, _) =>
        {
            if (type is PrimitiveType primitiveType) icebergTypeLookup.Add(id, primitiveType);
            return true;
        });

        var fieldIdMappingBuilder = ImmutableDictionary.CreateBuilder<int, FieldIdMappingValue>();
        using var schemaRoot = parquetSchema.SchemaRoot;
        schemaRoot.Visit(node =>
        {
            if (node is PrimitiveNode primitiveNode && primitiveNode.FieldId != -1)
                fieldIdMappingBuilder.Add(
                    primitiveNode.FieldId,
                    new FieldIdMappingValue(
                        parquetSchema.ColumnIndex(primitiveNode),
                        icebergTypeLookup[primitiveNode.FieldId]));
        });
        return new ParquetIcebergMetadata(fieldIdMappingBuilder.ToImmutable());
    }

    public static GroupNode FromSchema(Schema schema)
    {
        var nodes = new Node[schema.Fields.Count];
        for (var i = 0; i < schema.Fields.Count; i++)
        {
            var field = schema.Fields[i];
            var node = FromType(
                field.Name,
                field.FieldType,
                field.Required ? Repetition.Required : Repetition.Optional,
                field.Id);
            nodes[i] = node;
        }

        return new GroupNode("schema", Repetition.Required, nodes);
    }

    private static Node FromType(string name, IIcebergType type, Repetition repetition, int fieldId)
    {
        return type switch
        {
            // 3-Level List: Optional/Required Group (LIST) -> Repeated Group (list) -> Element
            ListType listType => new GroupNode(
                name,
                repetition,
                [
                    new GroupNode(
                        "list",
                        Repetition.Repeated,
                        [
                            FromType(
                                "element",
                                listType.Element,
                                listType.ElementRequired ? Repetition.Required : Repetition.Optional,
                                listType.ElementId)
                        ]) // Middle level usually has no ID in Iceberg
                ],
                LogicalType.List(),
                fieldId),

            // 3-Level Map: Optional/Required Group (MAP) -> Repeated Group (key_value) -> Key & Value
            MapType mapType => new GroupNode(
                name,
                repetition,
                [
                    new GroupNode(
                        "key_value",
                        Repetition.Repeated,
                        [
                            FromType("key", mapType.Key, Repetition.Required, mapType.KeyId),
                            FromType(
                                "value",
                                mapType.Value,
                                mapType.ValueRequired ? Repetition.Required : Repetition.Optional,
                                mapType.ValueId)
                        ])
                ],
                LogicalType.Map(),
                fieldId),

            StructType structType => new GroupNode(
                name,
                repetition,
                structType.Fields.Select(field => FromType(
                    field.Name,
                    field.FieldType,
                    field.Required ? Repetition.Required : Repetition.Optional,
                    field.Id)).ToList(),
                null,
                fieldId),

            PrimitiveType primitiveType => FromPrimitive(name, primitiveType, repetition, fieldId),

            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };
    }

    private static PrimitiveNode FromPrimitive(
        string name,
        PrimitiveType primitiveType,
        Repetition repetition,
        int fieldId)
    {
        primitiveType = PrimitiveType.Parse(primitiveType.Name);
        return primitiveType switch
        {
            PrimitiveType.Binary => new PrimitiveNode(
                name,
                repetition,
                LogicalType.None(),
                PhysicalType.ByteArray,
                0,
                fieldId),
            PrimitiveType.Boolean => new PrimitiveNode(
                name,
                repetition,
                LogicalType.None(),
                PhysicalType.Boolean,
                0,
                fieldId),
            PrimitiveType.Date => new PrimitiveNode(
                name,
                repetition,
                LogicalType.Date(),
                PhysicalType.Int32,
                0,
                fieldId),
            PrimitiveType.Decimal @decimal => FromDecimal(name, @decimal, repetition, fieldId),
            PrimitiveType.Double => new PrimitiveNode(
                name,
                repetition,
                LogicalType.None(),
                PhysicalType.Double,
                0,
                fieldId),
            PrimitiveType.Fixed @fixed => new PrimitiveNode(
                name,
                repetition,
                LogicalType.None(),
                PhysicalType.FixedLenByteArray,
                @fixed.L,
                fieldId),
            PrimitiveType.Float => new PrimitiveNode(
                name,
                repetition,
                LogicalType.None(),
                PhysicalType.Float,
                0,
                fieldId),
            PrimitiveType.Int => new PrimitiveNode(
                name,
                repetition,
                LogicalType.None(),
                PhysicalType.Int32,
                0,
                fieldId),
            PrimitiveType.Long => new PrimitiveNode(
                name,
                repetition,
                LogicalType.None(),
                PhysicalType.Int64,
                0,
                fieldId),
            PrimitiveType.String => new PrimitiveNode(
                name,
                repetition,
                LogicalType.String(),
                PhysicalType.ByteArray,
                0,
                fieldId),
            PrimitiveType.Time => new PrimitiveNode(
                name,
                repetition,
                LogicalType.Time(false, TimeUnit.Micros),
                PhysicalType.Float,
                0,
                fieldId),
            PrimitiveType.Timestamp => new PrimitiveNode(
                name,
                repetition,
                LogicalType.Timestamp(false, TimeUnit.Micros),
                PhysicalType.Int64,
                0,
                fieldId),
            PrimitiveType.TimestampNs => throw new NotImplementedException(),
            PrimitiveType.TimestampTz => new PrimitiveNode(
                name,
                repetition,
                LogicalType.Timestamp(false, TimeUnit.Micros),
                PhysicalType.Int64,
                0,
                fieldId),
            PrimitiveType.TimestampTzNs => throw new NotImplementedException(),
            PrimitiveType.Uuid => new PrimitiveNode(
                name,
                repetition,
                LogicalType.None(),
                PhysicalType.FixedLenByteArray,
                16,
                fieldId),
            _ => throw new ArgumentOutOfRangeException(nameof(primitiveType))
        };
    }

    private static PrimitiveNode FromDecimal(
        string name,
        PrimitiveType.Decimal type,
        Repetition repetition,
        int fieldId)
    {
        return type.P switch
        {
            <= 9 => new PrimitiveNode(
                name,
                repetition,
                LogicalType.Decimal(type.P, type.S),
                PhysicalType.Int32,
                0,
                fieldId),
            <= 18 => new PrimitiveNode(
                name,
                repetition,
                LogicalType.Decimal(type.P, type.S),
                PhysicalType.Int64,
                0,
                fieldId),
            _ => new PrimitiveNode(
                name,
                repetition,
                LogicalType.Decimal(type.P, type.S),
                PhysicalType.FixedLenByteArray,
                GetDecimalByteLength(type.P),
                fieldId)
        };
    }

    private static int GetDecimalByteLength(int precision)
    {
        // The formula for the number of bytes required for a given precision:
        // bytes = ceil(log2(10^precision - 1) / 8)
        // For simplicity and speed, most implementations use this standard mapping:
        return precision switch
        {
            <= 9 => 4, // Fits in Int32
            <= 18 => 8, // Fits in Int64
            <= 22 => 10,
            <= 26 => 12,
            <= 30 => 14,
            <= 34 => 16,
            <= 38 => 20,
            _ => throw new ArgumentException("Precision cannot exceed 38", nameof(precision))
        };
    }

    public record ParquetIcebergMetadata(ImmutableDictionary<int, FieldIdMappingValue> FieldIdMapping);

    public record FieldIdMappingValue(int ColumnIndex, PrimitiveType PrimitiveType);
}