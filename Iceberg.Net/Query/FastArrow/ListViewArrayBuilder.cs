using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Iceberg.Net.Query.FastArrow;

public class ListViewArrayBuilder : IArrowArrayBuilder<ListViewArray, ListViewArrayBuilder>
{
    public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

    public IArrowArray? ExistingValues { get; private set; }

    public int Length => ValueOffsetsBufferBuilder.Length;

    private ArrowBufferBuilder<int> ValueOffsetsBufferBuilder { get; }

    private ArrowBufferBuilder<int> SizesBufferBuilder { get; }

    private BitmapBuilder ValidityBufferBuilder { get; }

    public int NullCount { get; protected set; }

    private IArrowType DataType { get; }

    private int Start { get; set; }

    public ListViewArrayBuilder(IArrowType valueDataType) : this(new ListViewType(valueDataType))
    {
    }

    public ListViewArrayBuilder(Field valueField) : this(new ListViewType(valueField))
    {
    }

    internal ListViewArrayBuilder(ListViewType dataType, MemoryAllocator? allocator = null)
    {
        ValueBuilder = ArrowArrayBuilderFactory.Build(dataType.ValueDataType, allocator);
        ValueOffsetsBufferBuilder = new ArrowBufferBuilder<int>(8, allocator);
        SizesBufferBuilder = new ArrowBufferBuilder<int>(8, allocator);
        ValidityBufferBuilder = new BitmapBuilder(64, allocator);
        DataType = dataType;
        Start = -1;
    }

    // Shortcut when we are masking an existing list
    public ListViewArrayBuilder InitializeValuesFromList(ListArray l)
    {
        ExistingValues = l.Values;
        return this;
    }

    /// <summary>
    ///     Start a new variable-length list slot
    ///     This function should be called before beginning to append elements to the
    ///     value builder. TODO: Consider adding builder APIs to support construction
    ///     of overlapping lists.
    /// </summary>
    public ListViewArrayBuilder Append()
    {
        AppendPrevious();

        ValidityBufferBuilder.Append(true);

        return this;
    }

    public ListViewArrayBuilder AppendNull()
    {
        AppendPrevious();

        ValidityBufferBuilder.Append(false);
        ValueOffsetsBufferBuilder.Append(Start);
        SizesBufferBuilder.Append(0);
        NullCount++;
        Start = -1;

        return this;
    }

    public ListViewArrayBuilder AppendSized(int offset, int length)
    {
        if (Start >= 0)
        {
            ValueOffsetsBufferBuilder.Append(offset);
            SizesBufferBuilder.Append(length);
        }

        Start = offset + length + 1;
        return this;
    }

    private void AppendPrevious()
    {
        if (Start >= 0)
        {
            ValueOffsetsBufferBuilder.Append(Start);
            SizesBufferBuilder.Append(ValueBuilder.Length - Start);
        }

        Start = ValueBuilder.Length;
    }

    public ListViewArray Build(MemoryAllocator? allocator = null)
    {
        AppendPrevious();

        ArrowBuffer validityBuffer = NullCount > 0
            ? ValidityBufferBuilder.Build(allocator)
            : ArrowBuffer.Empty;

        return new ListViewArray(
            DataType,
            Length,
            ValueOffsetsBufferBuilder.Build(allocator),
            SizesBufferBuilder.Build(allocator),
            ExistingValues ?? ValueBuilder.Build(allocator),
            validityBuffer,
            NullCount);
    }

    public ListViewArrayBuilder Reserve(int capacity)
    {
        ValueOffsetsBufferBuilder.Reserve(capacity);
        SizesBufferBuilder.Reserve(capacity);
        ValidityBufferBuilder.Reserve(capacity);
        return this;
    }

    public ListViewArrayBuilder Resize(int length)
    {
        ValueOffsetsBufferBuilder.Resize(length);
        SizesBufferBuilder.Resize(length);
        ValidityBufferBuilder.Resize(length);
        return this;
    }

    public ListViewArrayBuilder Clear()
    {
        ValueOffsetsBufferBuilder.Clear();
        SizesBufferBuilder.Clear();
        ValueBuilder.Clear();
        ValidityBufferBuilder.Clear();
        ExistingValues = null;
        return this;
    }
}