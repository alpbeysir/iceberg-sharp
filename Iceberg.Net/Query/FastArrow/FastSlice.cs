using Apache.Arrow;
using Array = Apache.Arrow.Array;

namespace Iceberg.Net.Query.FastArrow;

public static class FastSlice
{
    public static void SliceInPlace(Array array, int offset, int length)
    {
        var newData = SliceData(array.Data, offset, length);
    }

    private static ArrayData SliceData(ArrayData data, int offset, int length)
    {
        if (offset > data.Length)
            throw new ArgumentException($"Offset {offset} cannot be greater than Length {data.Length} for Array.Slice");

        length = Math.Min(data.Length - offset, length);
        offset += data.Offset;

        int nullCount;
        if (data.NullCount == 0)
            nullCount = 0;
        else if (data.NullCount == data.Length)
            nullCount = length;
        else if (offset == data.Offset && length == data.Length)
            nullCount = data.NullCount;
        else
            nullCount = -1;

        return new ArrayData(data.DataType, length, nullCount, offset, data.Buffers, data.Children, data.Dictionary);
    }
}