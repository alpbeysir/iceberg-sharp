using System.Runtime.CompilerServices;
using Apache.Arrow;

namespace Iceberg.Net.Query.FastArrow;

public static class StructArrayHacks
{
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_fields")]
    private static extern ref IReadOnlyList<IArrowArray>? GetFields(StructArray array);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IReadOnlyList<IArrowArray> FastFields(this StructArray array)
    {
        IReadOnlyList<IArrowArray>? fields = GetFields(array);
        if (fields == null) return array.Fields;
        return fields;
    }
}