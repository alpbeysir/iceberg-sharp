using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Schemas;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Misc;

public static class Utils
{
    // Stops if this returns false
    public delegate bool IcebergTypeVisitor(IIcebergType type, int repetition, int id, bool required);

    public const string InitialBranch = "main";
    private static readonly Random Rd = new();

    public static void PrintTree(INode tree, int indent = 0)
    {
        switch (tree)
        {
            case Namespace ns:
                Console.WriteLine($"{ns.Identifier.GetLastIdentifier()}".PadLeft(indent, '-'));
                foreach (INode child in ns.Children.ToBlockingEnumerable()) PrintTree(child, indent + 4);
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
        for (int i = 0; i < suffixes.Length; i++)
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

        Type? maybe = Nullable.GetUnderlyingType(type);
        unwrapped = maybe ?? type;
        return maybe is not null;
    }


    public static string RandomCreateString(int stringLength)
    {
        const string allowedChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz0123456789!@$?_-";
        Span<char> chars = stackalloc char[stringLength];

        for (int i = 0; i < stringLength; i++) chars[i] = allowedChars[Rd.Next(0, allowedChars.Length)];

        return new string(chars);
    }

    public static void SaveToFile(Stream stream, string path)
    {
        long pos = stream.Position;
        stream.Seek(0, SeekOrigin.Begin);
        using FileStream file = new(path, FileMode.Create);
        stream.CopyTo(file);
        stream.Seek(pos, SeekOrigin.Begin);
    }

    public static Dictionary<string, MemberInfo> GetMembersByName(Type cSharpType)
    {
        return cSharpType.GetMembers(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public)
            .Where(m => m is PropertyInfo or FieldInfo)
            .ToDictionary(info => info.Name);
    }

    public static Type PropertyOrFieldType(MemberInfo member)
    {
        return member is PropertyInfo p1 ? p1.PropertyType : ((FieldInfo)member).FieldType;
    }

    public static long GenerateSnapshotId()
    {
        Guid uuid = Guid.NewGuid();
        byte[] bytes = uuid.ToByteArray();
        long mostSignificantBits = BitConverter.ToInt64(bytes, 0);
        long leastSignificantBits = BitConverter.ToInt64(bytes, 8);
        return (mostSignificantBits ^ leastSignificantBits) & long.MaxValue;
    }

    public static bool ImplementsInterface(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        this Type type,
        Type iface)
    {
        return type.IsAssignableTo(iface) || (type.IsGenericType && type.GetGenericTypeDefinition() == iface) || type
            .GetInterfaces()
            .Any(x => x.IsAssignableTo(iface) || (
                x.IsGenericType &&
                x.GetGenericTypeDefinition() == iface));
    }

    public static string PrettyPrint<T>(this T[] array, int edgeItems = 3)
    {
        if (array.Length <= edgeItems * 2) return $"[{string.Join(", ", array)}]";

        IEnumerable<T> head = array.Take(edgeItems);
        IEnumerable<T> tail = array.Skip(array.Length - edgeItems);

        return $"[{string.Join(", ", head)}, ..., {string.Join(", ", tail)}]";
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

    extension(IIcebergType icebergType)
    {
        private void Visit(IcebergTypeVisitor visitor, int repetition = 0, int id = -1, bool required = true)
        {
            visitor(icebergType, repetition, id, required);
            switch (icebergType)
            {
                case ListType listType:
                    listType.Element.Visit(visitor, repetition + 1, listType.ElementId, listType.ElementRequired);
                    break;
                case MapType mapType:
                    mapType.Key.Visit(visitor, repetition + 1, mapType.KeyId);
                    mapType.Value.Visit(visitor, repetition + 1, mapType.ValueId, mapType.ValueRequired);
                    break;
                case PrimitiveType:
                    break;
                case Schema schema:
                    foreach (StructField field in schema.Fields)
                        field.FieldType.Visit(visitor, repetition, field.Id, field.Required);
                    break;
                case StructType structType:
                    foreach (StructField field in structType.Fields)
                        field.FieldType.Visit(visitor, repetition, field.Id, field.Required);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(icebergType));
            }
        }
    }
}
