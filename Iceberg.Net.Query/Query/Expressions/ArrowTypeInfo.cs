using Apache.Arrow;
using Apache.Arrow.Types;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.FastArrow;
using Iceberg.Net.Schemas;
using ListType = Apache.Arrow.Types.ListType;

namespace Iceberg.Net.Query.Expressions;

public record ArrowTypeInfo(IArrowType ArrowType, Type ArrayType, Type BuilderType, Type CSharpType);

public static class ArrowTypeUtils
{
    // 1. Single source of truth using IArrowType as the map key
    private static readonly Dictionary<IArrowType, ArrowTypeInfo> ByArrowType = new()
    {
        {
            Int32Type.Default,
            new ArrowTypeInfo(Int32Type.Default, typeof(Int32Array), typeof(Int32Array.Builder), typeof(int))
        },
        {
            DoubleType.Default,
            new ArrowTypeInfo(DoubleType.Default, typeof(DoubleArray), typeof(DoubleArray.Builder), typeof(double))
        },
        {
            BooleanType.Default,
            new ArrowTypeInfo(BooleanType.Default, typeof(BooleanArray), typeof(BooleanArrayBuilder), typeof(bool))
        }
    };

    // 2. Secondary index automatically built from the primary map
    private static readonly Dictionary<Type, ArrowTypeInfo> ByCSharpType = new();

    static ArrowTypeUtils()
    {
        foreach (ArrowTypeInfo info in ByArrowType.Values) ByCSharpType[info.CSharpType] = info;
    }

    /// <summary>
    ///     Look up info using an Apache Arrow data type descriptor (handles nested ListTypes).
    /// </summary>
    internal static ArrowTypeInfo ForArrowType(IArrowType arrowType)
    {
        if (arrowType is ListType listType) return ListOf(ForArrowType(listType.ValueDataType));

        if (!ByArrowType.TryGetValue(arrowType, out ArrowTypeInfo? info))
            throw new NotSupportedException($"The Arrow type '{arrowType.GetType().Name}' is not supported.");

        return info;
    }

    /// <summary>
    ///     Look up info using a C# type (handles nested IEnumerable structures).
    /// </summary>
    internal static ArrowTypeInfo ForCSharpType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        // Recursively handle generic enumerable structures (like IEnumerable<T> or List<T>)
        if (type.IsGenericType && type.ImplementsInterface(typeof(IEnumerable<>)))
            return ListOf(ForCSharpType(type.GetGenericArguments()[0]));

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
        {
            IArrowType outputType = ArrowSchemas.FromIcebergType(CSharpSchemas.ToIcebergType(type, s => -1, ""));
            return new ArrowTypeInfo(outputType, typeof(StructArray), typeof(StructArrayBuilder), type);
        }

        if (!ByCSharpType.TryGetValue(type, out ArrowTypeInfo? info))
            throw new NotSupportedException($"The C# type '{type.FullName}' is not supported.");

        return info;
    }

    internal static ArrowTypeInfo ListOf(ArrowTypeInfo elementType)
    {
        return new ArrowTypeInfo(
            new ListType(elementType.ArrowType),
            typeof(ListArray),
            typeof(ListArrayBuilder),
            typeof(IEnumerable<>).MakeGenericType(elementType.CSharpType));
    }
}