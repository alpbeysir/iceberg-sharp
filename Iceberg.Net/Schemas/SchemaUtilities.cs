using Iceberg.Net.Metadata;

namespace Iceberg.Net.Schemas;

internal static class SchemaUtilities
{
    internal static IReadOnlyDictionary<int, IIcebergType> FieldsById(Schema schema)
    {
        Dictionary<int, IIcebergType> fields = new();
        AddFields(schema.Fields, fields);
        return fields;
    }

    internal static IReadOnlyList<PrimitiveType> PartitionTypes(
        IReadOnlyDictionary<int, IIcebergType> fields,
        PartitionSpec partitionSpec)
    {
        return partitionSpec.Fields
            .Select(field => PartitionResultType(
                fields.TryGetValue(field.SourceId, out IIcebergType? sourceType)
                    ? sourceType
                    : throw new InvalidDataException(
                        $"Partition field '{field.Name}' refers to unknown source field ID {field.SourceId}."),
                field.Transform))
            .ToArray();
    }

    private static PrimitiveType PartitionResultType(IIcebergType sourceType, string transform)
    {
        PrimitiveType source = sourceType is PrimitiveType primitive
            ? PrimitiveType.Parse(primitive.Name)
            : throw new NotSupportedException(
                $"Partition transform '{transform}' cannot be applied to non-primitive type {sourceType}.");

        if (transform == "identity" || transform == "void" ||
            transform.StartsWith("truncate[", StringComparison.Ordinal))
            return source;
        if (transform == "year" || transform == "month" || transform == "day" || transform == "hour" ||
            transform.StartsWith("bucket[", StringComparison.Ordinal))
            return new PrimitiveType.Int();

        throw new NotSupportedException($"Unknown Iceberg partition transform '{transform}'.");
    }

    private static void AddFields(
        IEnumerable<StructField> source,
        IDictionary<int, IIcebergType> destination)
    {
        foreach (StructField field in source)
        {
            destination.Add(field.Id, field.FieldType);
            AddNested(field.FieldType, destination);
        }
    }

    private static void AddNested(IIcebergType type, IDictionary<int, IIcebergType> destination)
    {
        while (true)
        {
            switch (type)
            {
                case StructType structType:
                    AddFields(structType.Fields, destination);
                    break;
                case ListType listType:
                    destination.Add(listType.ElementId, listType.Element);
                    type = listType.Element;
                    continue;
                case MapType mapType:
                    destination.Add(mapType.KeyId, mapType.Key);
                    AddNested(mapType.Key, destination);
                    destination.Add(mapType.ValueId, mapType.Value);
                    type = mapType.Value;
                    continue;
            }

            break;
        }
    }
}
