using System.Text;

namespace Iceberg.Net.Misc;

public static class PrettyPrintExtensions
{
    public static string PrettyPrint(this Type type, int recursionLevel = -1, bool expandNullable = false)
    {
        if (type.IsArray) return $"{PrettyPrint(type.GetElementType()!, recursionLevel, expandNullable)}[]";

        if (type.IsGenericType)
        {
            // find generic type name
            var genTypeName = type.GetGenericTypeDefinition().Name;
            var index = genTypeName.IndexOf('`');
            if (index != -1)
                genTypeName = genTypeName[..index];

            // retrieve generic type arguments
            var genTypeArgs = type.GetGenericArguments();
            var argNames = genTypeArgs.Select(genTypeArg => recursionLevel != 0
                    ? PrettyPrint(genTypeArg, recursionLevel - 1, expandNullable)
                    : "?")
                .ToList();

            // if type is nullable and want compact notation '?'
            if (!expandNullable && Nullable.GetUnderlyingType(type) != null)
                return $"{argNames[0]}?";

            // compose common generic type format "T<T1, T2, ...>"
            return $"{genTypeName}<{string.Join(", ", argNames)}>";
        }

        return type.Name;
    }

    public static string ToPrettyString<T>(in T[] array, int edgeItems = 3)
    {
        var len = array.Length;
        var sb = new StringBuilder();
        sb.Append('[');

        if (len <= edgeItems * 2)
        {
            BuildRange(sb, array, 0, len);
        }
        else
        {
            BuildRange(sb, array, 0, edgeItems);
            sb.Append(", ..., ");
            BuildRange(sb, array, len - edgeItems, edgeItems);
        }

        return sb.Append(']').ToString();
    }

    public static string ToPrettyMapString<TK, TV>(TK[] keys, TV[] values, int edgeItems = 3)
    {
        if (keys.Length != values.Length)
            throw new ArgumentException("Arrays must be of the same length.");

        var len = keys.Length;
        var sb = new StringBuilder();
        sb.Append('{');

        if (len <= edgeItems * 2)
        {
            BuildMapRange(sb, keys, values, 0, len);
        }
        else
        {
            BuildMapRange(sb, keys, values, 0, edgeItems);
            sb.Append(", ..., ");
            BuildMapRange(sb, keys, values, len - edgeItems, edgeItems);
        }

        return sb.Append('}').ToString();
    }

    // Helper for single arrays to avoid code duplication
    private static void BuildRange<T>(StringBuilder sb, T[] array, int start, int count)
    {
        for (var i = 0; i < count; i++)
        {
            sb.Append(array[start + i]);
            if (i < count - 1) sb.Append(", ");
        }
    }

    // Helper for map-style arrays
    private static void BuildMapRange<TK, TV>(StringBuilder sb, TK[] keys, TV[] values, int start, int count)
    {
        for (var i = 0; i < count; i++)
        {
            var idx = start + i;
            sb.Append(keys[idx]).Append(": ").Append(values[idx]);
            if (i < count - 1) sb.Append(", ");
        }
    }
}