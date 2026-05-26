using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Memory;

namespace Iceberg.Net.Query.FastArrow;

public class ArrowBufferBuilder<T> where T : struct
{
    public MemoryAllocator? Allocator { get; }
    private const int DefaultCapacity = 8;

    private readonly int _size;

    /// <summary>
    ///     Gets the number of items that can be contained in the memory allocated by the current instance.
    /// </summary>
    public int Capacity => Memory.Length / _size;

    /// <summary>
    ///     Gets the number of items currently appended.
    /// </summary>
    public int Length { get; private set; }

    /// <summary>
    ///     Gets the raw byte memory underpinning the builder.
    /// </summary>
    public Memory<byte> Memory { get; private set; }

    /// <summary>
    ///     Gets the span of memory underpinning the builder.
    /// </summary>
    public Span<T> Span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Memory.Span.CastTo<T>();
    }

    /// <summary>
    ///     Creates an instance of the <see cref="ArrowBufferBuilder{T}" /> class.
    /// </summary>
    /// <param name="capacity">Number of items of initial capacity to reserve.</param>
    public ArrowBufferBuilder(int capacity = DefaultCapacity, MemoryAllocator? allocator = null)
    {
        // Using `bool` as the template argument, if used in an unrestricted fashion, would result in a buffer
        // with inappropriate contents being produced.  Because C# does not support template specialization,
        // and because generic type constraints do not support negation, we will throw a runtime error to
        // indicate that such a template type is not supported.
        if (typeof(T) == typeof(bool))
            throw new NotSupportedException(
                $"An instance of {nameof(ArrowBufferBuilder<T>)} cannot be instantiated, as `bool` is not an " +
                $"appropriate generic type to use with this class - please use {nameof(BitmapBuilder)} " +
                $"instead");
        Allocator = allocator;

        _size = Unsafe.SizeOf<T>();

        Memory = Allocator is not null
            ? Allocator.Allocate(capacity).Memory
            : new Memory<byte>(new byte[capacity]);
        Length = 0;
    }

    /// <summary>
    ///     Append a buffer, assumed to contain items of the same type.
    /// </summary>
    /// <param name="buffer">Buffer to append.</param>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public ArrowBufferBuilder<T> Append(ArrowBuffer buffer)
    {
        Append(buffer.Span.CastTo<T>());
        return this;
    }

    /// <summary>
    ///     Append a single item.
    /// </summary>
    /// <param name="value">Item to append.</param>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public ArrowBufferBuilder<T> Append(T value)
    {
        EnsureAdditionalCapacity(1);
        Span[Length++] = value;
        return this;
    }

    /// <summary>
    ///     Append a span of items.
    /// </summary>
    /// <param name="source">Source of item span.</param>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public ArrowBufferBuilder<T> Append(ReadOnlySpan<T> source)
    {
        EnsureAdditionalCapacity(source.Length);
        source.CopyTo(Span.Slice(Length, source.Length));
        Length += source.Length;
        return this;
    }

    /// <summary>
    ///     Append a number of items.
    /// </summary>
    /// <param name="values">Items to append.</param>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public ArrowBufferBuilder<T> AppendRange(IEnumerable<T> values)
    {
        foreach (var v in values)
            Append(v);

        return this;
    }

    /// <summary>
    ///     Reserve a given number of items' additional capacity.
    /// </summary>
    /// <param name="additionalCapacity">Number of items of required additional capacity.</param>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public ArrowBufferBuilder<T> Reserve(int additionalCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(additionalCapacity);

        EnsureAdditionalCapacity(additionalCapacity);
        return this;
    }

    /// <summary>
    ///     Resize the buffer to a given size.
    /// </summary>
    /// <remarks>
    ///     Note that if the required capacity is larger than the current length of the populated buffer so far,
    ///     the buffer's contents in the new, expanded region are undefined.
    /// </remarks>
    /// <remarks>
    ///     Note that if the required capacity is smaller than the current length of the populated buffer so far,
    ///     the buffer will be truncated and items at the end of the buffer will be lost.
    /// </remarks>
    /// <param name="capacity">Number of items of required capacity.</param>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public ArrowBufferBuilder<T> Resize(int capacity)
    {
        if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be non-negative");

        EnsureCapacity(capacity);
        Length = capacity;

        return this;
    }

    /// <summary>
    ///     Clear all contents appended so far.
    /// </summary>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public ArrowBufferBuilder<T> Clear()
    {
        Span.Fill(default);
        Length = 0;
        return this;
    }

    /// <summary>
    ///     Build an Arrow buffer from the appended contents so far.
    /// </summary>
    /// <param name="allocator">Optional memory allocator.</param>
    /// <returns>Returns an <see cref="ArrowBuffer" /> object.</returns>
    public ArrowBuffer Build(MemoryAllocator? allocator = null)
    {
        return Build(64, allocator);
    }

    /// <summary>
    ///     Build an Arrow buffer from the appended contents so far of the specified byte size.
    /// </summary>
    /// <param name="allocator">Optional memory allocator.</param>
    /// <returns>Returns an <see cref="ArrowBuffer" /> object.</returns>
    internal ArrowBuffer Build(int byteSize, MemoryAllocator? allocator = null)
    {
        var currentBytesLength = Length * _size;
        var bufferLength = checked((int)BitUtility.RoundUpToMultiplePowerOfTwo(currentBytesLength, byteSize));

        var memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
        var memoryOwner = memoryAllocator.Allocate(bufferLength);
        Memory[..currentBytesLength].CopyTo(memoryOwner.Memory);

        return new ArrowBuffer(memoryOwner.Memory);
    }

    private void EnsureAdditionalCapacity(int additionalCapacity)
    {
        EnsureCapacity(checked(Length + additionalCapacity));
    }

    private void EnsureCapacity(int requiredCapacity)
    {
        if (requiredCapacity > Capacity)
        {
            // TODO: specifiable growth strategy
            // Double the length of the in-memory array, or use the byte count of the capacity, whichever is
            // greater.
            var capacity = Math.Max(requiredCapacity * _size, Memory.Length * 2);
            Reallocate(capacity);
        }
    }

    private void Reallocate(int numBytes)
    {
        if (numBytes != 0)
        {
            var memory = Allocator is not null
                ? Allocator.Allocate(numBytes).Memory
                : new Memory<byte>(new byte[numBytes]);
            Memory.CopyTo(memory);

            Memory = memory;
        }
    }
}