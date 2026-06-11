using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Iceberg.Net.Query.FastArrow;

public class ListArrayBuilder : IArrowArrayBuilder<ListArray, ListArrayBuilder>
{
    public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

    public int Length => ValueOffsetsBufferBuilder.Length;

    private ArrowBufferBuilder<int> ValueOffsetsBufferBuilder { get; }

    private BitmapBuilder ValidityBufferBuilder { get; }

    public int NullCount { get; protected set; }

    private IArrowType DataType { get; }

    public ListArrayBuilder(IArrowType valueDataType) : this(new ListType(valueDataType))
    {
    }

    public ListArrayBuilder(Field valueField) : this(new ListType(valueField))
    {
    }

    internal ListArrayBuilder(ListType dataType) : this(dataType, null)
    {
    }

    internal ListArrayBuilder(ListType dataType, MemoryAllocator? allocator = null)
    {
        ValueBuilder = ArrowArrayBuilderFactory.Build(dataType.ValueDataType, allocator);
        ValueOffsetsBufferBuilder = new ArrowBufferBuilder<int>(8, allocator);
        ValidityBufferBuilder = new BitmapBuilder(64, allocator);
        DataType = dataType;
    }

    // Shortcut when the value builder has already been appended to
    public ListArrayBuilder InitializeFromList(ListArray l)
    {
        ValueOffsetsBufferBuilder.Append(l.ValueOffsets[..^1]);
        ValidityBufferBuilder.Append(l.NullBitmapBuffer.Span, l.NullBitmapBuffer.Length);
        return this;
    }

    /// <summary>
    ///     Start a new variable-length list slot
    ///     This function should be called before beginning to append elements to the
    ///     value builder
    /// </summary>
    /// <returns></returns>
    public ListArrayBuilder Append()
    {
        ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);
        ValidityBufferBuilder.Append(true);

        return this;
    }

    public ListArrayBuilder AppendNull()
    {
        ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);
        ValidityBufferBuilder.Append(false);
        NullCount++;

        return this;
    }

    public ListArray Build(MemoryAllocator? allocator = null)
    {
        ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);

        ArrowBuffer validityBuffer = NullCount > 0
            ? ValidityBufferBuilder.Build(allocator)
            : ArrowBuffer.Empty;

        return new ListArray(
            DataType,
            Length - 1,
            ValueOffsetsBufferBuilder.Build(allocator),
            ValueBuilder.Build(allocator),
            validityBuffer,
            NullCount);
    }

    public ListArrayBuilder Reserve(int capacity)
    {
        ValueOffsetsBufferBuilder.Reserve(capacity + 1);
        ValidityBufferBuilder.Reserve(capacity);
        return this;
    }

    public ListArrayBuilder Resize(int length)
    {
        ValueOffsetsBufferBuilder.Resize(length + 1);
        ValidityBufferBuilder.Resize(length);
        return this;
    }

    public ListArrayBuilder Clear()
    {
        ValueOffsetsBufferBuilder.Clear();
        ValueBuilder.Clear();
        ValidityBufferBuilder.Clear();
        return this;
    }
}