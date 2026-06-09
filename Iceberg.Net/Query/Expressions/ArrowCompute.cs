using System.Diagnostics;
using System.Linq.Expressions;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.FastArrow;
using Varena;
using ZLinq.Simd;
using BitUtility = Apache.Arrow.BitUtility;

namespace Iceberg.Net.Query.Expressions;

public static class ArrowCompute
{
    // TODO this will not handle nulls properly
    public static ReadOnlySpan<T> Zip<T>(
        ExecutionContext ctx,
        ReadOnlySpan<T> l,
        ReadOnlySpan<T> r,
        ExpressionType expressionType)
        where T : unmanaged, INumber<T>
    {
        if (l.IsEmpty) return ReadOnlySpan<T>.Empty;
        // TODO precompute these switches
        Func<Vector<T>, Vector<T>, Vector<T>> vectorSelector = expressionType switch
        {
            ExpressionType.Add => Vector.Add,
            ExpressionType.Subtract => Vector.Subtract,
            ExpressionType.Multiply => Vector.Multiply,
            ExpressionType.Divide => Vector.Divide,
            ExpressionType.GreaterThan => Vector.GreaterThan,
            ExpressionType.LessThan => Vector.LessThan,
            ExpressionType.And => Vector.BitwiseAnd,
            ExpressionType.AndAlso => Vector.BitwiseAnd,
            ExpressionType.Equal => Vector.Equals,
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
        Func<T, T, T> selector = expressionType switch
        {
            ExpressionType.Add => (n1, n2) => n1 + n2,
            ExpressionType.Subtract => (n1, n2) => n1 - n2,
            ExpressionType.Multiply => (n1, n2) => n1 * n2,
            ExpressionType.Divide => (n1, n2) => n1 / n2,
            ExpressionType.GreaterThan => (n1, n2) => FromBoolMask<T>(n1 > n2),
            ExpressionType.LessThan => (n1, n2) => FromBoolMask<T>(n1 < n2),
            ExpressionType.And => (n1, n2) => UnsafeBitwise(n1, n2, (i, i1) => i & i1, (l1, l2) => l1 & l2),
            ExpressionType.AndAlso => (n1, n2) => UnsafeBitwise(n1, n2, (i, i1) => i & i1, (l1, l2) => l1 & l2),
            ExpressionType.Equal => (n1, n2) => FromBoolMask<T>(n1 == n2),
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
        ZipVectorizable<T, T> zip = l.AsVectorizable().Zip(
            r,
            vectorSelector,
            selector);
        Span<T> result = ArenaAllocate<T>(ctx.Arena, l.Length);
        zip.CopyTo(result);
        return result;
    }

    public static ReadOnlySpan<T> SliceSpan<T>(ReadOnlySpan<T> span, Range range)
    {
        (int Offset, int Length) offsetAndLength = range.GetOffsetAndLength(span.Length);
        return span.Slice(offsetAndLength.Offset, offsetAndLength.Length);
    }

    private static T UnsafeBitwise<T>(T l, T r, Func<int, int, int> func, Func<long, long, long> func2)
        where T : unmanaged, INumber<T>
    {
        if (Unsafe.SizeOf<T>() == sizeof(long))
        {
            var llong = Unsafe.As<T, long>(ref l);
            var rlong = Unsafe.As<T, long>(ref r);
            var res = func2(llong, rlong);
            return Unsafe.As<long, T>(ref res);
        }

        if (Unsafe.SizeOf<T>() == sizeof(int))
        {
            var lint = Unsafe.As<T, int>(ref l);
            var rint = Unsafe.As<T, int>(ref r);
            var res = func(lint, rint);
            return Unsafe.As<int, T>(ref res);
        }

        throw new UnreachableException();
    }

    public static TResultBuilder ExecuteElementWiseListOpWithRange<TInput, TElementArray, TResultArray, TResultBuilder>(
        ExecutionContext ctx,
        TInput input,
        TResultBuilder builder,
        Action<ExecutionContext, RangedInput<TElementArray>, TResultBuilder> op)
        where TResultBuilder : IArrowArrayBuilder<TResultArray, TResultBuilder>
        where TElementArray : class, IArrowArray
        where TInput : IInput<ListArray>
        where TResultArray : class, IArrowArray
    {
        builder.Reserve(input.Length);
        ListArray l = input.Array;
        ListArrayBuilder? asListBuilder = builder as ListArrayBuilder;
        for (var i = 0; i < l.Length; i++)
        {
            asListBuilder?.Append();
            var start = l.ValueOffsets[i];
            var end = start + l.GetValueLength(i);
            Range range = new(start, end);
            op(ctx, new RangedInput<TElementArray>((TElementArray)l.Values, range), builder);
        }

        return builder;
    }

    // fast path when result is a list and we can reuse the original offsets
    public static ListArrayBuilder ExecuteOneToOneListOp<TInput, TElementArray>(
        ExecutionContext ctx,
        TInput input,
        ListArrayBuilder builder,
        Action<ExecutionContext, IdentityInput<TElementArray>, ListArrayBuilder> op) where TInput : IInput<ListArray>
        where TElementArray : IArrowArray
    {
        ListArray l = input.Array;
        builder.Reserve(l.Length);
        builder.ValueBuilder.Reserve(l.Values.Length);
        IdentityInput<TElementArray> values = new((TElementArray)l.Values);
        op(ctx, values, builder);
        builder.InitializeFromList(l);
        return builder;
    }

    public static TResultBuilder MakeBuilderForGeneric<TResultBuilder>(IArrowType arrowType, MemoryAllocator? allocator)
        where TResultBuilder : class, IArrowArrayBuilder
    {
        return (TResultBuilder)MakeBuilderFor(arrowType, allocator);
    }

    public static IArrowArrayBuilder<IArrowArray> MakeBuilderFor(IArrowType arrowType, MemoryAllocator? allocator)
    {
        return arrowType switch
        {
            DoubleType => new DoubleArray.Builder(),
            Int32Type => new Int32Array.Builder(),
            BooleanType => new BooleanArrayBuilder(allocator),
            ListType l => new ListArrayBuilder(l, allocator),
            StructType s => new StructArrayBuilder(s, allocator),
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), arrowType, null)
        };
    }

    public static ReadOnlySpan<byte> BitmapOps(
        ExecutionContext ctx,
        ReadOnlySpan<byte> l,
        ReadOnlySpan<byte> r,
        ExpressionType expressionType)
    {
        // TODO precompute these switches
        Func<Vector<byte>, Vector<byte>, Vector<byte>> vectorSelector = expressionType switch
        {
            ExpressionType.And => Vector.BitwiseAnd,
            ExpressionType.AndAlso => Vector.BitwiseAnd,
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
        Func<byte, byte, byte> selector = expressionType switch
        {
            ExpressionType.And or ExpressionType.AndAlso => (n1, n2) => (byte)(n1 & n2),
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
        ZipVectorizable<byte, byte> zip = l.AsVectorizable().Zip(
            r,
            vectorSelector,
            selector);
        Span<byte> result = ArenaAllocate<byte>(ctx.Arena, l.Length);
        zip.CopyTo(result);
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T FromBoolMask<T>(bool value) where T : struct, INumber<T>
    {
        // If you need a SIMD-style mask (all bits set for true), 
        // we subtract 1 from 0 (results in -1, or all bits set in two's complement).
        T val = value ? T.One : T.Zero;

        // This creates -1 for true (0xFF...) and 0 for false (0x00...)
        return T.Zero - val;
    }

    private static Span<T> ArenaAllocate<T>(VirtualBuffer buffer, int amount) where T : struct
    {
        Console.WriteLine($"compute alloc {Utils.ToFileSize(amount)}");
        return MemoryMarshal.Cast<byte, T>(buffer.AllocateRange(Unsafe.SizeOf<T>() * amount));
    }

    public static ReadOnlySpan<TResult> ConvertLogical<T, TResult>(
        ExecutionContext ctx,
        ReadOnlySpan<T> buffer)
        where T : struct, INumber<T>
        where TResult : struct, INumber<TResult>
    {
        Span<TResult> result = ArenaAllocate<TResult>(ctx.Arena, buffer.Length);
        for (var i = 0; i < buffer.Length; i++) result[i] = TResult.CreateChecked(buffer[i]);

        return result;
    }

    private static readonly int VectorSize = Vector<byte>.Count;

    public static bool All(BooleanArray array, Range range)
    {
        ReadOnlySpan<byte> bitmap = array.Values;

        (int Offset, int Length) offsetAndLength = range.GetOffsetAndLength(array.Length);
        var currentBit = offsetAndLength.Offset;
        var bitsRemaining = offsetAndLength.Length;

        if (bitsRemaining <= 0) return true;

        // 1. Handle Head (Unsynchronized bits up to the next byte boundary)
        var headBits = (8 - (currentBit & 7)) & 7;
        if (headBits > 0)
        {
            var bitsToRead = Math.Min(headBits, bitsRemaining);
            if (!MatchScalar(bitmap, currentBit, bitsToRead, true))
                return false;

            currentBit += bitsToRead;
            bitsRemaining -= bitsToRead;
        }

        if (bitsRemaining <= 0) return true;

        // Move to byte-based tracking
        var byteIndex = currentBit >> 3;
        var byteLength = bitsRemaining >> 3;
        var tailBits = bitsRemaining & 7;

        // 2. Vectorized Loop (Process full Vector chunks)
        if (Vector.IsHardwareAccelerated && byteLength >= VectorSize)
        {
            // Vector of 0xFF (all bits set)
            Vector<byte> allOnes = Vector<byte>.AllBitsSet;

            while (byteLength >= VectorSize)
            {
                Vector<byte> vector = new(bitmap.Slice(byteIndex, VectorSize));

                // If any byte in the vector is not 0xFF, Vector.Equals won't match allOnes
                if (!Vector.EqualsAll(vector, allOnes))
                    return false;

                byteIndex += VectorSize;
                byteLength -= VectorSize;
            }
        }

        // 3. Handle Middle Tail (Remaining full bytes less than a Vector size)
        while (byteLength > 0)
        {
            if (bitmap[byteIndex] != 0xFF)
                return false;

            byteIndex++;
            byteLength--;
        }

        // 4. Handle Tail Bits (The remaining fractional bits at the end)
        if (tailBits > 0)
        {
            currentBit = byteIndex << 3;
            if (!MatchScalar(bitmap, currentBit, tailBits, true))
                return false;
        }

        return true;
    }

    public static bool Any(BooleanArray array, Range range)
    {
        ReadOnlySpan<byte> bitmap = array.Values;

        (int Offset, int Length) offsetAndLength = range.GetOffsetAndLength(array.Length);
        var currentBit = offsetAndLength.Offset;
        var bitsRemaining = offsetAndLength.Length;

        // 1. Handle Head
        var headBits = (8 - (currentBit & 7)) & 7;
        if (headBits > 0)
        {
            var bitsToRead = Math.Min(headBits, bitsRemaining);
            if (MatchScalar(bitmap, currentBit, bitsToRead, false))
                return true;

            currentBit += bitsToRead;
            bitsRemaining -= bitsToRead;
        }
        
        if (bitsRemaining <= 0) return false;

        var byteIndex = currentBit >> 3;
        var byteLength = bitsRemaining >> 3;
        var tailBits = bitsRemaining & 7;

        // 2. Vectorized Loop
        if (Vector.IsHardwareAccelerated && byteLength >= VectorSize)
        {
            Vector<byte> allZeros = Vector<byte>.Zero;

            while (byteLength >= VectorSize)
            {
                Vector<byte> vector = new(bitmap.Slice(byteIndex, VectorSize));

                // If the vector is NOT entirely zeros, then it contains at least one '1' bit
                if (!Vector.EqualsAll(vector, allZeros))
                    return true;

                byteIndex += VectorSize;
                byteLength -= VectorSize;
            }
        }

        // 3. Handle Middle Tail
        while (byteLength > 0)
        {
            if (bitmap[byteIndex] != 0x00)
                return true;

            byteIndex++;
            byteLength--;
        }

        // 4. Handle Tail Bits
        if (tailBits > 0)
        {
            currentBit = byteIndex << 3;
            if (MatchScalar(bitmap, currentBit, tailBits, false))
                return true;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool MatchScalar(ReadOnlySpan<byte> bitmap, int bitOffset, int length, bool expected)
    {
        var byteIndex = bitOffset >> 3;
        var bitPos = bitOffset & 7;

        // Create a mask for the bits we care about in this byte
        // e.g., if bitPos = 2 and length = 3, we want bits 2, 3, and 4.
        var mask = (byte)(((1 << length) - 1) << bitPos);
        var value = (byte)(bitmap[byteIndex] & mask);

        if (expected)
            // For 'All', the masked bits must match the mask itself (all 1s)
            return value == mask;
        else
            // For 'Any', the masked bits must not be completely 0
            return value != 0;
    }

    public static ReadOnlySpan<byte> BitmapFromMask<T>(ExecutionContext ctx, ReadOnlySpan<T> mask)
        where T : struct, INumber<T>
    {
        ReadOnlySpan<T> source = mask;
        Span<byte> destination = ArenaAllocate<byte>(ctx.Arena, mask.Length);

        var vectorSize = Vector<T>.Count;

        for (var i = 0; i <= source.Length - vectorSize; i += vectorSize)
        {
            // Ensure destination is large enough
            var bytesNeeded = (i + vectorSize + 7) / 8;
            if (destination.Length < bytesNeeded)
                throw new ArgumentException("Destination span is too small.");

            Vector<T> vec = new(source[i..(i + vectorSize)]);

            for (var j = 0; j < vectorSize; j++)
                // In SIMD masks, 'True' means all bits set.
                // We check if the lane is non-zero to treat it as 'True'.
                if (vec[j] != T.Zero)
                {
                    var currentBit = i + j;
                    BitUtility.SetBit(destination, currentBit);
                    // destination[currentBit >> 3] |= (byte)(1 << (currentBit & 7));
                }
        }

        var startIndex = source.Length - source.Length % vectorSize;
        var remaining = source.Length % vectorSize;

        if (remaining > 0)
            for (var i = 0; i < remaining; i++)
                if (source[startIndex + i] != T.Zero)
                    BitUtility.SetBit(destination, startIndex + i);

        return destination;
    }
}