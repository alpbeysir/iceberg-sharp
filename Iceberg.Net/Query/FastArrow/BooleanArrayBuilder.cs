using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Memory;

namespace Iceberg.Net.Query.FastArrow;

public class BooleanArrayBuilder(MemoryAllocator? allocator = null)
    : IArrowArrayBuilder<bool, BooleanArray, BooleanArrayBuilder>
{
    private BitmapBuilder ValueBuffer { get; } = new(64, allocator);
    private BitmapBuilder ValidityBuffer { get; } = new(64, allocator);

    public int Length => ValueBuffer.Length;
    public int Capacity => ValueBuffer.Capacity;
    public int NullCount => ValidityBuffer.UnsetBitCount;

    public BooleanArrayBuilder Append(bool value)
    {
        return NullableAppend(value);
    }

    public BooleanArrayBuilder NullableAppend(bool? value)
    {
        // Note that we rely on the fact that null values are false in the value buffer.
        ValueBuffer.Append(value ?? false);
        ValidityBuffer.Append(value.HasValue);
        return this;
    }

    public BooleanArrayBuilder Append(ReadOnlySpan<bool> span)
    {
        foreach (var value in span) Append(value);
        return this;
    }

    public BooleanArrayBuilder AppendMask<TMask>(ReadOnlySpan<TMask> mask)
        where TMask : unmanaged, INumber<TMask>
    {
        foreach (var value in mask) Append(value != TMask.Zero);
        return this;
    }

    public BooleanArrayBuilder AppendRange(IEnumerable<bool> values)
    {
        foreach (var value in values) Append(value);
        return this;
    }

    public BooleanArrayBuilder AppendNull()
    {
        return NullableAppend(null);
    }

    public BooleanArray Build(MemoryAllocator? allocator = null)
    {
        var validityBuffer = NullCount > 0
            ? ValidityBuffer.Build(allocator)
            : ArrowBuffer.Empty;

        return new BooleanArray(
            ValueBuffer.Build(allocator),
            validityBuffer,
            Length,
            NullCount,
            0);
    }

    public BooleanArrayBuilder Clear()
    {
        ValueBuffer.Clear();
        ValidityBuffer.Clear();
        return this;
    }

    public BooleanArrayBuilder Reserve(int capacity)
    {
        if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));

        ValueBuffer.Reserve(capacity);
        ValidityBuffer.Reserve(capacity);
        return this;
    }

    public BooleanArrayBuilder Resize(int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));

        ValueBuffer.Resize(length);
        ValidityBuffer.Resize(length);
        return this;
    }

    public BooleanArrayBuilder Toggle(int index)
    {
        CheckIndex(index);

        // If there is a null at this index, assume it was set to false in the value buffer, and so becomes
        // true/non-null after toggling.
        ValueBuffer.Toggle(index);
        ValidityBuffer.Set(index);
        return this;
    }

    public BooleanArrayBuilder Set(int index)
    {
        CheckIndex(index);
        ValueBuffer.Set(index);
        ValidityBuffer.Set(index);
        return this;
    }

    public BooleanArrayBuilder Set(int index, bool value)
    {
        CheckIndex(index);
        ValueBuffer.Set(index, value);
        ValidityBuffer.Set(index);
        return this;
    }

    public BooleanArrayBuilder Swap(int i, int j)
    {
        CheckIndex(i);
        CheckIndex(j);
        ValueBuffer.Swap(i, j);
        ValidityBuffer.Swap(i, j);
        return this;
    }

    private void CheckIndex(int index)
    {
        if (index < 0 || index >= Length) throw new ArgumentOutOfRangeException(nameof(index));
    }
}