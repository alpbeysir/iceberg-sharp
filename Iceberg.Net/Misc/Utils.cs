using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Schemas;
using ParquetSharp.Schema;

namespace Iceberg.Net.Misc;

public static class Utils
{
    // Stops if this returns false
    public delegate bool IcebergTypeVisitor(IIcebergType type, int repetition, int id, bool required);

    public const string InitialBranch = "main";
    public static readonly RecordSchema EmptyPartitionAvroSchema = RecordSchema.Create("r102", []);
    private static readonly Random Rd = new();

    public static void PrintTree(INode tree, int indent = 0)
    {
        switch (tree)
        {
            case Namespace ns:
                Console.WriteLine($"{ns.Identifier.GetLastIdentifier()}".PadLeft(indent, '-'));
                foreach (var child in ns.Children.ToBlockingEnumerable()) PrintTree(child, indent + 4);
                break;
            case Table table:
                Console.WriteLine($"{table.Identifier.GetLastIdentifier()}".PadLeft(indent, '-'));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(tree));
        }
    }

    // https://csharphelper.com/howtos/howto_file_size_in_words.html
    public static string ToFileSize(this double value)
    {
        string[] suffixes =
        [
            "bytes", "KB", "MB", "GB",
            "TB", "PB", "EB", "ZB", "YB"
        ];
        for (var i = 0; i < suffixes.Length; i++)
            if (value <= Math.Pow(1024, i + 1))
                return ThreeNonZeroDigits(
                           value /
                           Math.Pow(1024, i)) +
                       " " + suffixes[i];

        return ThreeNonZeroDigits(
                   value /
                   Math.Pow(1024, suffixes.Length - 1)) +
               " " + suffixes[^1];
    }

    private static string ThreeNonZeroDigits(double value)
    {
        return value switch
        {
            // No digits after the decimal.
            >= 100 => value.ToString("0,0"),
            // One digit after the decimal.
            >= 10 => value.ToString("0.0"),
            _ => value.ToString("0.00")
        };

        // Two digits after the decimal.
    }

    public static bool IsNullable(Type? type, [NotNullIfNotNull(nameof(type))] out Type? unwrapped)
    {
        if (type is null)
        {
            unwrapped = null;
            return false;
        }

        var maybe = Nullable.GetUnderlyingType(type);
        unwrapped = maybe ?? type;
        return maybe is not null;
    }


    public static string RandomCreateString(int stringLength)
    {
        const string allowedChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz0123456789!@$?_-";
        Span<char> chars = stackalloc char[stringLength];

        for (var i = 0; i < stringLength; i++) chars[i] = allowedChars[Rd.Next(0, allowedChars.Length)];

        return new string(chars);
    }

    public static void SaveToFile(Stream stream, string path)
    {
        var pos = stream.Position;
        stream.Seek(0, SeekOrigin.Begin);
        using var file = new FileStream(path, FileMode.Create);
        stream.CopyTo(file);
        stream.Seek(pos, SeekOrigin.Begin);
    }

    public static Dictionary<string, MemberInfo> GetMembersByName(Type cSharpType)
    {
        return cSharpType.GetMembers(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public)
            .Where(m => m is PropertyInfo or FieldInfo)
            .ToDictionary(info => info.Name);
    }

    internal static Type PropertyOrFieldType(MemberInfo member)
    {
        return member is PropertyInfo p1 ? p1.PropertyType : ((FieldInfo)member).FieldType;
    }

    public static string GetParquetFileName(int num, int unknown, Guid guid)
    {
        return $"{num:D5}-{unknown}-{guid}.parquet";
    }

    public static long GenerateSnapshotId()
    {
        var uuid = Guid.NewGuid();
        var bytes = uuid.ToByteArray();
        var mostSignificantBits = BitConverter.ToInt64(bytes, 0);
        var leastSignificantBits = BitConverter.ToInt64(bytes, 8);
        return (mostSignificantBits ^ leastSignificantBits) & long.MaxValue;
    }

    public static bool ImplementsInterface(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        this Type type,
        Type iface)
    {
        return type.GetInterfaces().Any(x => x.IsAssignableTo(iface) || (
            x.IsGenericType &&
            x.GetGenericTypeDefinition() == iface));
    }

    public static string PrettyPrint<T>(this T[] array, int edgeItems = 3)
    {
        if (array.Length <= edgeItems * 2) return $"[{string.Join(", ", array)}]";

        var head = array.Take(edgeItems);
        var tail = array.Skip(array.Length - edgeItems);

        return $"[{string.Join(", ", head)}, ..., {string.Join(", ", tail)}]";
    }

    public static Expression ForExpression(
        ParameterExpression loopVar,
        Expression initValue,
        Expression condition,
        Expression increment,
        Expression loopContent)
    {
        var initAssign = Expression.Assign(loopVar, initValue);
        var breakLabel = Expression.Label("LoopBreak");

        return Expression.Block(
            [loopVar],
            initAssign,
            Expression.Loop(
                Expression.IfThenElse(
                    condition,
                    Expression.Block(
                        loopContent,
                        increment),
                    Expression.Break(breakLabel)),
                breakLabel)
        );
    }

    extension(Content content)
    {
        public string ToMetadataString()
        {
            return content switch
            {
                Content.Data => "data",
                Content.Deletes => "deletes",
                _ => throw new ArgumentOutOfRangeException(nameof(content), content, null)
            };
        }
    }


    extension(Node node)
    {
        public void Visit(Action<Node> visitor)
        {
            visitor(node);
            if (node is GroupNode groupNode)
                foreach (var child in groupNode.Fields)
                {
                    Visit(child, visitor);
                    child.Dispose();
                }
        }
    }

    extension(IIcebergType icebergType)
    {
        public void Visit(IcebergTypeVisitor visitor, int repetition = 0, int id = -1, bool required = true)
        {
            visitor(icebergType, repetition, id, required);
            switch (icebergType)
            {
                case ListType listType:
                    Visit(listType.Element, visitor, repetition + 1, listType.ElementId, listType.ElementRequired);
                    break;
                case MapType mapType:
                    Visit(mapType.Key, visitor, repetition + 1, mapType.KeyId);
                    Visit(mapType.Value, visitor, repetition + 1, mapType.ValueId, mapType.ValueRequired);
                    break;
                case PrimitiveType primitiveType:
                    break;
                case Schemas.Schema schema:
                    foreach (var field in schema.Fields)
                        Visit(field.FieldType, visitor, repetition, field.Id, field.Required);
                    break;
                case StructType structType:
                    foreach (var field in structType.Fields)
                        Visit(field.FieldType, visitor, repetition, field.Id, field.Required);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(icebergType));
            }
        }
    }
}