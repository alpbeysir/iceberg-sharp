using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Memory;

namespace Iceberg.Net.Query.FastArrow;

public class BitmapBuilder
{
    private const int DefaultBitCapacity = 64;
    private static readonly int VectorSize = Vector<byte>.Count;

    // --- Optimization State ---
    private ulong _stagingBuffer; // Acts as a fast CPU-register local buffer (up to 64 bits)
    private int _stagingCount; // Tracks how many bits are currently pending in the staging buffer

    public int Capacity { get; private set; }
    public MemoryAllocator? Allocator { get; }
    public int Length { get; private set; }
    public Memory<byte> Memory { get; private set; }
    public Span<byte> Span => Memory.Span;
    public int SetBitCount { get; private set; }
    public int UnsetBitCount => Length - SetBitCount;

    public BitmapBuilder(int capacity = DefaultBitCapacity) : this(capacity, null)
    {
    }

    public BitmapBuilder(int capacity = DefaultBitCapacity, MemoryAllocator? allocator = null)
    {
        Capacity = capacity;
        Allocator = allocator;
        Memory = Allocator is not null
            ? Allocator.Allocate(BitUtility.ByteCount(capacity)).Memory
            : new byte[BitUtility.ByteCount(capacity)];
    }

    /// <summary>
    ///     Optimized: Append a single bit using an ultra-fast bit shift register.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public BitmapBuilder Append(bool value)
    {
        // 1. Shift the bit into our 64-bit CPU register staging buffer
        var bit = value ? 1UL : 0UL;
        _stagingBuffer |= bit << _stagingCount;
        _stagingCount++;
        SetBitCount += (int)bit;
        Length++;

        // 2. Once we hit 64 bits (or 8 bytes), batch-flush it directly to memory
        if (_stagingCount == 64) FlushStagingBuffer();

        return this;
    }

    /// <summary>
    ///     Optimized bulk append using vectorized (SIMD) bit counting.
    /// </summary>
    public BitmapBuilder Append(ReadOnlySpan<byte> source, int validBits)
    {
        if (!source.IsEmpty && validBits > source.Length * 8)
            throw new ArgumentException($"Valid bits ({validBits}) cannot exceed source size.", nameof(validBits));

        if (validBits <= 0) return this;

        // Force a flush of our local bit-staging buffer if it isn't byte-aligned
        // so we can perform fast byte-aligned copies/writes.
        if (_stagingCount % 8 != 0) FlushPartialStagingBytes();

        // If we are cleanly byte-aligned now, we can perform a fast blit copy
        if (_stagingCount == 0 && Length % 8 == 0)
        {
            EnsureAdditionalCapacity(validBits);

            var targetByteOffset = Length / 8;
            var bytesToCopy = BitUtility.ByteCount(validBits);

            source[..bytesToCopy].CopyTo(Span[targetByteOffset..]);

            Length += validBits;
            // High performance SIMD counting replacement for BitUtility.CountBits
            SetBitCount += CountBitsSimd(source[..bytesToCopy], validBits); 
        }
        else
        {
            // Fallback fallback if bits are completely unaligned
            for (var i = 0; i < validBits; i++)
                Append(source.IsEmpty || BitUtility.GetBit(source, i));
        }

        return this;
    }

    /// <summary>
    ///     Forces any remaining bits in the local CPU staging buffer to be written to memory.
    /// </summary>
    public void Flush()
    {
        if (_stagingCount > 0) FlushPartialStagingBytes();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FlushStagingBuffer()
    {
        EnsureAdditionalCapacity(0); // Check capacity before writing 8 bytes
        var targetByteIndex = (Length - 64) / 8;

        // Write full 8-byte ulong directly down to memory
        MemoryMarshal.Write(Span[targetByteIndex..], in _stagingBuffer);

        _stagingBuffer = 0;
        _stagingCount = 0;
    }

    private void FlushPartialStagingBytes()
    {
        var bytesToWrite = (_stagingCount + 7) / 8;
        EnsureAdditionalCapacity(0);

        var targetByteIndex = (Length - _stagingCount) / 8;
        var tempBuffer = _stagingBuffer;

        for (var i = 0; i < bytesToWrite; i++)
        {
            Span[targetByteIndex + i] = (byte)(tempBuffer & 0xFF);
            tempBuffer >>= 8;
        }

        // Adjust remaining bits down
        _stagingCount = 0;
        _stagingBuffer = 0;
    }

    /// <summary>
    ///     SIMD Optimized calculation of population count (Set Bits) across raw bytes
    /// </summary>
    private static int CountBitsSimd(ReadOnlySpan<byte> bytes, int totalValidBits)
    {
        var count = 0;
        var i = 0;

        // Process with hardware SIMD vectors if available
        if (Vector.IsHardwareAccelerated && bytes.Length >= VectorSize)
            while (i <= bytes.Length - VectorSize)
            {
                var vector = new Vector<byte>(bytes.Slice(i, VectorSize));
                // Vectorized population count across all elements
                for (var j = 0; j < VectorSize; j++) count += BitOperations.PopCount(vector[j]);
                i += VectorSize;
            }

        // Tail cleanup loop
        for (; i < bytes.Length; i++) count += BitOperations.PopCount(bytes[i]);

        // Clean up excess bit padding if the trailing byte wasn't fully utilized
        var totalExpectedBytes = BitUtility.ByteCount(totalValidBits);
        var structuralBitsInLastByte = totalValidBits % 8;
        if (structuralBitsInLastByte > 0 && bytes.Length >= totalExpectedBytes)
        {
            var lastByte = bytes[totalExpectedBytes - 1];
            var excessBits = 8 - structuralBitsInLastByte;
            var maskedOutBits = (byte)(lastByte >> structuralBitsInLastByte);
            count -= BitOperations.PopCount(maskedOutBits);
        }

        return count;
    }

    // Ensure any API alterations or reads flush structural components
    public ArrowBuffer Build(MemoryAllocator? allocator = null)
    {
        Flush();
        if (Allocator == allocator) return new ArrowBuffer(Memory);
        var bufferLength = checked((int)BitUtility.RoundUpToMultipleOf64(Memory.Length));
        var memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
        var memoryOwner = memoryAllocator.Allocate(bufferLength);
        Memory[..].CopyTo(memoryOwner.Memory);
        return new ArrowBuffer(memoryOwner.Memory);
    }

    public BitmapBuilder Clear()
    {
        Span.Clear();
        Length = 0;
        SetBitCount = 0;
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
    /// <param name="capacity">Number of bits of required capacity.</param>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public BitmapBuilder Resize(int capacity)
    {
        if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be non-negative");

        EnsureCapacity(capacity);
        Length = capacity;

        SetBitCount = BitUtility.CountBits(Span, 0, Length);

        return this;
    }
    
    /// <summary>
    ///     Reserve a given number of bits' additional capacity.
    /// </summary>
    /// <param name="additionalCapacity">Number of bits of required additional capacity.</param>
    /// <returns>Returns the builder (for fluent-style composition).</returns>
    public BitmapBuilder Reserve(int additionalCapacity)
    {
        if (additionalCapacity < 0) throw new ArgumentOutOfRangeException(nameof(additionalCapacity));
        EnsureAdditionalCapacity(additionalCapacity);
        return this;
    }


    private void EnsureAdditionalCapacity(int additionalCapacity)
    {
        // Add 64 bits to structural validation to protect register-flushing sizes safely
        EnsureCapacity(checked(Length + additionalCapacity + 64));
    }

    private void EnsureCapacity(int requiredCapacity)
    {
        if (requiredCapacity > Capacity)
        {
            var byteCount = Math.Max(BitUtility.ByteCount(requiredCapacity), Memory.Length * 2);
            Reallocate(byteCount);
            Capacity = byteCount * 8;
        }
    }

    private void Reallocate(int numBytes)
    {
        if (numBytes != 0)
        {
            Debug.Assert(numBytes > Memory.Length);
            var memory = Allocator is not null
                ? Allocator.Allocate(numBytes).Memory
                : new Memory<byte>(new byte[numBytes]);
            Memory.CopyTo(memory);
            Memory = memory;
        }
    }

    // Make sure to call Flush() inside implementation items like AppendRange, Toggle, Set, or Clear.
}