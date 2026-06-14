using System.Diagnostics;
using System.Linq.Expressions;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Iceberg.Net.Query.FastArrow;
using Varena;
using BitUtility = Apache.Arrow.BitUtility;

namespace Iceberg.Net.Query.Expressions;

public enum MemoryConfig
{
    /// <summary>Allocate new memory for the result.</summary>
    None,

    /// <summary>Reuse the left operand's memory for the result.</summary>
    Left,

    /// <summary>Reuse the right operand's memory for the result.</summary>
    Right
}

/// <summary>
///     A span-backed bitmap with a known logical bit count.
///     The <see cref="Bytes" /> span contains packed bits (1 bit per boolean),
///     and <see cref="Length" /> tracks the exact number of logical bits,
///     which may be less than <c>Bytes.Length * 8</c>.
/// </summary>
public readonly ref struct Bitmap
{
    public readonly Span<byte> Bytes;
    public readonly int Length; // logical bit count

    public Bitmap(Span<byte> bytes, int length)
    {
        Bytes = bytes;
        Length = length;
    }

    public bool IsEmpty => Length == 0 || Bytes.IsEmpty;
    public static Bitmap Empty => default;
}

public static class ArrowCompute
{
    /// <summary>
    ///     Creates a writable <see cref="Span{T}" /> from a <see cref="ReadOnlySpan{T}" />.
    ///     Use this only when the underlying memory is known to be mutable (e.g., arena-allocated buffers).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Span<T> AsWritable<T>(ReadOnlySpan<T> span)
    {
        return Unsafe.BitCast<ReadOnlySpan<T>, Span<T>>(span);
    }

    /// <summary>
    ///     Converts a <see cref="Span{T}" /> to <see cref="ReadOnlySpan{T}" />.
    ///     Exists so expression trees can perform the implicit conversion explicitly.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<T> AsReadOnlySpan<T>(Span<T> span)
    {
        return span;
    }

    /// <summary>
    ///     Wraps raw bitmap bytes with a known logical bit count into a <see cref="Bitmap" />.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Bitmap AsBitmap(Span<byte> bytes, int bitLength)
    {
        return new Bitmap(bytes, bitLength);
    }

    // TODO this will not handle nulls properly
    public static Span<T> Zip<T>(
        ExecutionContext ctx,
        Span<T> l,
        Span<T> r,
        ExpressionType expressionType,
        MemoryConfig memConfig = MemoryConfig.None)
        where T : unmanaged, INumber<T>
    {
        if (l.IsEmpty) return Span<T>.Empty;

        var len = l.Length;
        Span<T> result = memConfig switch
        {
            MemoryConfig.Left => l[..len],
            MemoryConfig.Right => r[..len],
            _ => ArenaAllocate<T>(ctx.Arena, len)
        };

        Func<Vector<T>, Vector<T>, Vector<T>> vectorSelector = GetVectorOp<T>(expressionType);
        Func<T, T, T> scalarSelector = GetScalarOp<T>(expressionType);

        var i = 0;
        var vecSize = Vector<T>.Count;

        if (Vector.IsHardwareAccelerated && len >= vecSize)
        {
            var vecEnd = len - len % vecSize;
            for (; i < vecEnd; i += vecSize)
            {
                Vector<T> vl = new(l.Slice(i, vecSize));
                Vector<T> vr = new(r.Slice(i, vecSize));
                vectorSelector(vl, vr).CopyTo(result.Slice(i, vecSize));
            }
        }

        for (; i < len; i++) result[i] = scalarSelector(l[i], r[i]);

        return result;
    }

    /// <summary>
    ///     Binary operation where the left operand is a scalar and the right is a span.
    /// </summary>
    public static Span<T> ZipScalarLeft<T>(
        ExecutionContext ctx,
        T left,
        Span<T> right,
        ExpressionType expressionType,
        MemoryConfig memConfig = MemoryConfig.None)
        where T : unmanaged, INumber<T>
    {
        if (right.IsEmpty) return Span<T>.Empty;

        var len = right.Length;
        Span<T> result = memConfig switch
        {
            MemoryConfig.Right => right[..len],
            _ => ArenaAllocate<T>(ctx.Arena, len)
        };

        Func<Vector<T>, Vector<T>, Vector<T>> vectorSelector = GetVectorOp<T>(expressionType);
        Func<T, T, T> scalarSelector = GetScalarOp<T>(expressionType);
        Vector<T> scalarVec = new(left);

        var i = 0;
        var vecSize = Vector<T>.Count;

        if (Vector.IsHardwareAccelerated && right.Length >= vecSize)
        {
            var vecEnd = right.Length - right.Length % vecSize;
            for (; i < vecEnd; i += vecSize)
            {
                Vector<T> vr = new(right.Slice(i, vecSize));
                vectorSelector(scalarVec, vr).CopyTo(result.Slice(i, vecSize));
            }
        }

        for (; i < right.Length; i++) result[i] = scalarSelector(left, right[i]);

        return result;
    }

    /// <summary>
    ///     Binary operation where the left operand is a span and the right is a scalar.
    /// </summary>
    public static Span<T> ZipScalarRight<T>(
        ExecutionContext ctx,
        Span<T> left,
        T right,
        ExpressionType expressionType,
        MemoryConfig memConfig = MemoryConfig.None)
        where T : unmanaged, INumber<T>
    {
        if (left.IsEmpty) return Span<T>.Empty;

        var len = left.Length;
        Span<T> result = memConfig switch
        {
            MemoryConfig.Left => left[..len],
            _ => ArenaAllocate<T>(ctx.Arena, len)
        };

        Func<Vector<T>, Vector<T>, Vector<T>> vectorSelector = GetVectorOp<T>(expressionType);
        Func<T, T, T> scalarSelector = GetScalarOp<T>(expressionType);
        Vector<T> scalarVec = new(right);

        var i = 0;
        var vecSize = Vector<T>.Count;

        if (Vector.IsHardwareAccelerated && left.Length >= vecSize)
        {
            var vecEnd = left.Length - left.Length % vecSize;
            for (; i < vecEnd; i += vecSize)
            {
                Vector<T> vl = new(left.Slice(i, vecSize));
                vectorSelector(vl, scalarVec).CopyTo(result.Slice(i, vecSize));
            }
        }

        for (; i < left.Length; i++) result[i] = scalarSelector(left[i], right);

        return result;
    }

    // Used by generated expression trees to read from ReadOnlySpan<int> by index
    // (expression trees can't handle ref returns from get_Item)
    public static int ReadOffset(ReadOnlySpan<int> offsets, int index) => offsets[index];

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

    public static TResultBuilder ExecuteElementWiseListOp<TInput, TResultArray, TResultBuilder>(
        ExecutionContext ctx,
        TInput input,
        Action<ExecutionContext, RangedInput, TResultBuilder> op,
        TResultBuilder builder)
        where TResultBuilder : IArrowArrayBuilder<TResultArray, TResultBuilder>
        where TInput : IInput<TInput>
        where TResultArray : class, IArrowArray
    {
        Debug.Assert(typeof(TInput) == typeof(IdentityInput));

        builder.Reserve(input.Length);
        ListArray l = (ListArray)input.Array;
        ListArrayBuilder? asListBuilder = builder as ListArrayBuilder;
        for (var i = 0; i < l.Length; i++)
        {
            asListBuilder?.Append();
            var start = l.ValueOffsets[i];
            var end = start + l.GetValueLength(i);
            Range range = new(start, end);
            op(ctx, new RangedInput(l.Values, range), builder);
        }

        return builder;
    }

    // fast path when result is a list and we can reuse the original offsets
    public static ListArrayBuilder ExecuteListSelect<TInput>(
        ExecutionContext ctx,
        TInput input,
        Action<ExecutionContext, TInput, ListArrayBuilder> op,
        ListArrayBuilder builder)
        where TInput : IInput<TInput>
    {
        Debug.Assert(typeof(TInput) == typeof(IdentityInput));

        ListArray l = (ListArray)input.Array;

        builder.Reserve(l.Length);
        builder.ValueBuilder.Reserve(l.Values.Length);

        TInput subInput = input.Apply(l.Values);
        op(ctx, subInput, builder);

        // TODO use input type here
        builder.InitializeOffsetsFromList(l, 0, l.Length);

        return builder;
    }

    public static ListArrayBuilder ExecuteListWhere<TInput>(
        ExecutionContext ctx,
        TInput input,
        // TODO allow this function to ask for another builder type
        Action<ExecutionContext, TInput, BooleanArrayBuilder> op,
        Action<ExecutionContext, IndexedInput, ListArrayBuilder> copier,
        ListArrayBuilder builder)
        where TInput : IInput<TInput>
    {
        Debug.Assert(typeof(TInput) == typeof(IdentityInput));

        BooleanArrayBuilder maskBuilder = new(ctx.ArrowAllocator);
        ListArray l = (ListArray)input.Array;

        TInput subInput = input.Apply(l.Values);
        op(ctx, subInput, maskBuilder);

        using BooleanArray mask = maskBuilder.Build(ctx.ArrowAllocator);

        Debug.Assert(mask.Length == l.Values.Length);
        Debug.Assert(mask.NullCount == 0);

        for (var i = 0; i < l.Length; i++)
        {
            builder.Append();
            var start = l.ValueOffsets[i];
            var end = start + l.GetValueLength(i);
            for (var j = start; j < end; j++)
            {
                if (!mask.GetValue(j)!.Value)
                    continue;

                IndexedInput indexedInput = new(l.Values, j);
                copier(ctx, indexedInput, builder);
            }
        }

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
            ListViewType lv => new ListViewArrayBuilder(lv, allocator),
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), arrowType, null)
        };
    }

    public static Bitmap BitmapOps(
        ExecutionContext ctx,
        Bitmap l,
        Bitmap r,
        ExpressionType expressionType,
        MemoryConfig memConfig = MemoryConfig.None)
    {
        var byteLen = l.Bytes.Length;

        Span<byte> resultBytes = memConfig switch
        {
            MemoryConfig.Left => l.Bytes,
            MemoryConfig.Right => r.Bytes,
            _ => ArenaAllocate<byte>(ctx.Arena, byteLen)
        };

        // Slice to exact byte length in case the reused span is larger
        resultBytes = resultBytes[..byteLen];

        Func<Vector<byte>, Vector<byte>, Vector<byte>> vectorSelector = GetByteVectorOp(expressionType);
        Func<byte, byte, byte> scalarSelector = GetByteScalarOp(expressionType);

        var i = 0;
        var vecSize = Vector<byte>.Count;

        if (Vector.IsHardwareAccelerated && byteLen >= vecSize)
        {
            var vecEnd = byteLen - byteLen % vecSize;
            for (; i < vecEnd; i += vecSize)
            {
                Vector<byte> vl = new(l.Bytes.Slice(i, vecSize));
                Vector<byte> vr = new(r.Bytes.Slice(i, vecSize));
                vectorSelector(vl, vr).CopyTo(resultBytes.Slice(i, vecSize));
            }
        }

        for (; i < byteLen; i++) resultBytes[i] = scalarSelector(l.Bytes[i], r.Bytes[i]);

        return new Bitmap(resultBytes, l.Length);
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
        // Console.WriteLine($"compute alloc {Utils.ToFileSize(amount)}");
        return MemoryMarshal.Cast<byte, T>(buffer.AllocateRange(Unsafe.SizeOf<T>() * amount));
    }

    public static Span<TResult> ConvertLogical<T, TResult>(
        ExecutionContext ctx,
        Span<T> buffer,
        MemoryConfig memConfig = MemoryConfig.None)
        where T : struct, INumber<T>
        where TResult : struct, INumber<TResult>
    {
        // Note: memConfig reuse only meaningful when T == TResult (same size)
        Span<TResult> result;
        if (memConfig != MemoryConfig.None && typeof(T) == typeof(TResult))
            result = MemoryMarshal.Cast<T, TResult>(buffer);
        else
            result = ArenaAllocate<TResult>(ctx.Arena, buffer.Length);

        // TODO optimize here by choosing unchecked if values are within range
        for (var i = 0; i < buffer.Length; i++) result[i] = TResult.CreateChecked(buffer[i]);

        return result;
    }

    private static readonly int VectorSize = Vector<byte>.Count;

    public static TBuilder CopyPrimitive<TInput, TArray, TValue, TBuilder>(
        ExecutionContext ctx,
        TInput input,
        TBuilder builder)
        where TInput : IInput<TInput>
        where TArray : PrimitiveArray<TValue>
        where TBuilder : PrimitiveArrayBuilder<TValue, TArray, TBuilder>
        where TValue : struct, IEquatable<TValue>
    {
        TArray array = (TArray)input.Array;

        if (input is IndexedInput indexed)
            return builder.Append(array.Values[indexed.Index]);

        Debug.Assert(typeof(TInput) == typeof(IdentityInput));
        return builder.Append(array.Values);
    }

    public static ListArrayBuilder CopyList<TInput>(
        ExecutionContext ctx,
        TInput input,
        ListArrayBuilder builder,
        Action<ExecutionContext, TInput, ListArrayBuilder> valueCopier)
        where TInput : IInput<TInput>
    {
        // TODO switch by input type
        Debug.Assert(typeof(TInput) == typeof(IdentityInput));

        ListArray l = (ListArray)input.Array;
        builder.InitializeOffsetsFromList(l, 0, input.Length);
        TInput subInput = input.Apply(l.Values);
        valueCopier(ctx, subInput, builder);
        return builder;
    }

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

    /// <summary>
    ///     Converts a SIMD mask span (all-bits-set = true, zero = false) into a packed bitmap.
    /// </summary>
    /// <returns>A <see cref="Bitmap" /> whose <see cref="Bitmap.Length" /> equals <paramref name="mask" />.Length.</returns>
    public static Bitmap BitmapFromMask<T>(
        ExecutionContext ctx,
        Span<T> mask)
        where T : struct, INumber<T>
    {
        var bitLength = mask.Length;
        var byteLength = (bitLength + 7) / 8;
        Span<byte> destination = ArenaAllocate<byte>(ctx.Arena, byteLength);

        var i = 0;
        var vecSize = Vector<T>.Count;

        if (Vector.IsHardwareAccelerated && mask.Length >= vecSize)
        {
            var vecEnd = mask.Length - mask.Length % vecSize;
            for (; i < vecEnd; i += vecSize)
            {
                Vector<T> vec = new(mask.Slice(i, vecSize));
                for (var j = 0; j < vecSize; j++)
                    if (vec[j] != T.Zero)
                        BitUtility.SetBit(destination, i + j);
            }
        }

        for (; i < mask.Length; i++)
            if (mask[i] != T.Zero)
                BitUtility.SetBit(destination, i);

        return new Bitmap(destination, bitLength);
    }

    #region Operation Dispatch Tables

    private static Func<Vector<T>, Vector<T>, Vector<T>> GetVectorOp<T>(ExpressionType expressionType)
        where T : unmanaged, INumber<T>
    {
        return expressionType switch
        {
            ExpressionType.Add => Vector.Add,
            ExpressionType.Subtract => Vector.Subtract,
            ExpressionType.Multiply => Vector.Multiply,
            ExpressionType.Divide => Vector.Divide,
            ExpressionType.GreaterThan => Vector.GreaterThan,
            ExpressionType.LessThan => Vector.LessThan,
            ExpressionType.GreaterThanOrEqual => (l, r) => Vector.GreaterThanOrEqual(l, r),
            ExpressionType.LessThanOrEqual => (l, r) => Vector.LessThanOrEqual(l, r),
            ExpressionType.Equal => Vector.Equals,
            ExpressionType.NotEqual => (l, r) => ~Vector.Equals(l, r),
            ExpressionType.And or ExpressionType.AndAlso => Vector.BitwiseAnd,
            ExpressionType.Or or ExpressionType.OrElse => Vector.BitwiseOr,
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
    }

    private static Func<T, T, T> GetScalarOp<T>(ExpressionType expressionType)
        where T : unmanaged, INumber<T>
    {
        return expressionType switch
        {
            ExpressionType.Add => (n1, n2) => n1 + n2,
            ExpressionType.Subtract => (n1, n2) => n1 - n2,
            ExpressionType.Multiply => (n1, n2) => n1 * n2,
            ExpressionType.Divide => (n1, n2) => n1 / n2,
            ExpressionType.GreaterThan => (n1, n2) => FromBoolMask<T>(n1 > n2),
            ExpressionType.LessThan => (n1, n2) => FromBoolMask<T>(n1 < n2),
            ExpressionType.GreaterThanOrEqual => (n1, n2) => FromBoolMask<T>(n1 >= n2),
            ExpressionType.LessThanOrEqual => (n1, n2) => FromBoolMask<T>(n1 <= n2),
            ExpressionType.Equal => (n1, n2) => FromBoolMask<T>(n1 == n2),
            ExpressionType.NotEqual => (n1, n2) => FromBoolMask<T>(n1 != n2),
            ExpressionType.And or ExpressionType.AndAlso => (n1, n2) =>
                UnsafeBitwise(n1, n2, (i, i1) => i & i1, (l1, l2) => l1 & l2),
            ExpressionType.Or or ExpressionType.OrElse => (n1, n2) =>
                UnsafeBitwise(n1, n2, (i, i1) => i | i1, (l1, l2) => l1 | l2),
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
    }

    private static Func<Vector<byte>, Vector<byte>, Vector<byte>> GetByteVectorOp(ExpressionType expressionType)
    {
        return expressionType switch
        {
            ExpressionType.And or ExpressionType.AndAlso => Vector.BitwiseAnd,
            ExpressionType.Or or ExpressionType.OrElse => Vector.BitwiseOr,
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
    }

    private static Func<byte, byte, byte> GetByteScalarOp(ExpressionType expressionType)
    {
        return expressionType switch
        {
            ExpressionType.And or ExpressionType.AndAlso => (n1, n2) => (byte)(n1 & n2),
            ExpressionType.Or or ExpressionType.OrElse => (n1, n2) => (byte)(n1 | n2),
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
    }

    #endregion
}
