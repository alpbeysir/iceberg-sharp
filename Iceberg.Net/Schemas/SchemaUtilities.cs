using Iceberg.Net.Metadata;

namespace Iceberg.Net.Schemas;

internal static class SchemaUtilities
{
    internal static IReadOnlyDictionary<string, int> FieldIdsByPath(Schema schema)
    {
        Dictionary<string, int> fields = new(StringComparer.Ordinal);
        AddFieldPaths(schema.Fields, string.Empty, fields);
        return fields;
    }

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

    private static void AddFieldPaths(
        IEnumerable<StructField> source,
        string pathPrefix,
        IDictionary<string, int> destination)
    {
        foreach (StructField field in source)
        {
            string path = string.IsNullOrEmpty(pathPrefix)
                ? field.Name
                : $"{pathPrefix}.{field.Name}";
            destination.Add(path, field.Id);
            AddNestedPaths(field.FieldType, path, destination);
        }
    }

    private static void AddNestedPaths(
        IIcebergType type,
        string path,
        IDictionary<string, int> destination)
    {
        switch (type)
        {
            case StructType structType:
                AddFieldPaths(structType.Fields, path, destination);
                break;
            case ListType listType:
                string elementPath = $"{path}.element";
                destination.Add(elementPath, listType.ElementId);
                AddNestedPaths(listType.Element, elementPath, destination);
                break;
            case MapType mapType:
                string keyPath = $"{path}.key";
                destination.Add(keyPath, mapType.KeyId);
                AddNestedPaths(mapType.Key, keyPath, destination);

                string valuePath = $"{path}.value";
                destination.Add(valuePath, mapType.ValueId);
                AddNestedPaths(mapType.Value, valuePath, destination);
                break;
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
