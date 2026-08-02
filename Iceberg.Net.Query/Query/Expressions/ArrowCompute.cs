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
    ///     Wraps a built <see cref="IArrowArray" /> into the appropriate <see cref="IInput{T}" /> wrapper.
    /// </summary>
    public static TInput BuildArray<TInput>(IArrowArray array)
        where TInput : IInput<TInput>
    {
        if (typeof(TInput) == typeof(IdentityInput))
            return (TInput)(object)IdentityInput.New(array);
        if (typeof(TInput) == typeof(IndexedInput))
            return (TInput)(object)new IndexedInput(array, 0);
        if (typeof(TInput) == typeof(RangedInput))
            return (TInput)(object)new RangedInput(array, Range.All);
        throw new InvalidOperationException($"Unsupported TInput: {typeof(TInput)}");
    }

    /// <summary>
    ///     Wraps raw bitmap bytes with a known logical bit count into a <see cref="Bitmap" />.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Bitmap AsBitmap(Span<byte> bytes, int bitLength)
    {
        return new Bitmap(bytes, bitLength);
    }

    /// <summary>
    ///     Allocates a span and fills it with <paramref name="value" /> repeated <paramref name="count" /> times.
    /// </summary>
    public static Span<T> FillSpan<T>(ExecutionContext ctx, T value, int count)
        where T : struct
    {
        if (count == 0) return Span<T>.Empty;

        Span<T> result = ArenaAllocate<T>(ctx.Arena, count);
        result.Fill(value);
        return result;
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

        int len = l.Length;
        Span<T> result = memConfig switch
        {
            MemoryConfig.Left => l[..len],
            MemoryConfig.Right => r[..len],
            _ => ArenaAllocate<T>(ctx.Arena, len)
        };

        Func<Vector<T>, Vector<T>, Vector<T>> vectorSelector = GetVectorOp<T>(expressionType);
        Func<T, T, T> scalarSelector = GetScalarOp<T>(expressionType);

        int i = 0;
        int vecSize = Vector<T>.Count;

        if (Vector.IsHardwareAccelerated && len >= vecSize)
        {
            int vecEnd = len - len % vecSize;
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

        int len = right.Length;
        Span<T> result = memConfig switch
        {
            MemoryConfig.Right => right[..len],
            _ => ArenaAllocate<T>(ctx.Arena, len)
        };

        Func<Vector<T>, Vector<T>, Vector<T>> vectorSelector = GetVectorOp<T>(expressionType);
        Func<T, T, T> scalarSelector = GetScalarOp<T>(expressionType);
        Vector<T> scalarVec = new(left);

        int i = 0;
        int vecSize = Vector<T>.Count;

        if (Vector.IsHardwareAccelerated && right.Length >= vecSize)
        {
            int vecEnd = right.Length - right.Length % vecSize;
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

        int len = left.Length;
        Span<T> result = memConfig switch
        {
            MemoryConfig.Left => left[..len],
            _ => ArenaAllocate<T>(ctx.Arena, len)
        };

        Func<Vector<T>, Vector<T>, Vector<T>> vectorSelector = GetVectorOp<T>(expressionType);
        Func<T, T, T> scalarSelector = GetScalarOp<T>(expressionType);
        Vector<T> scalarVec = new(right);

        int i = 0;
        int vecSize = Vector<T>.Count;

        if (Vector.IsHardwareAccelerated && left.Length >= vecSize)
        {
            int vecEnd = left.Length - left.Length % vecSize;
            for (; i < vecEnd; i += vecSize)
            {
                Vector<T> vl = new(left.Slice(i, vecSize));
                vectorSelector(vl, scalarVec).CopyTo(result.Slice(i, vecSize));
            }
        }

        for (; i < left.Length; i++) result[i] = scalarSelector(left[i], right);

        return result;
    }

    private static T UnsafeBitwise<T>(T l, T r, Func<int, int, int> func, Func<long, long, long> func2)
        where T : unmanaged, INumber<T>
    {
        if (Unsafe.SizeOf<T>() == sizeof(long))
        {
            long llong = Unsafe.As<T, long>(ref l);
            long rlong = Unsafe.As<T, long>(ref r);
            long res = func2(llong, rlong);
            return Unsafe.As<long, T>(ref res);
        }

        if (Unsafe.SizeOf<T>() == sizeof(int))
        {
            int lint = Unsafe.As<T, int>(ref l);
            int rint = Unsafe.As<T, int>(ref r);
            int res = func(lint, rint);
            return Unsafe.As<int, T>(ref res);
        }

        throw new UnreachableException();
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
            // TODO make our versions of these builders that don't waste memory
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
        int byteLen = l.Bytes.Length;

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

        int i = 0;
        int vecSize = Vector<byte>.Count;

        if (Vector.IsHardwareAccelerated && byteLen >= vecSize)
        {
            int vecEnd = byteLen - byteLen % vecSize;
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
        if (buffer.IsEmpty) return Span<TResult>.Empty;
        
        // Note: memConfig reuse only meaningful when T == TResult (same size)
        Span<TResult> result;
        if (memConfig != MemoryConfig.None && typeof(T) == typeof(TResult))
            result = MemoryMarshal.Cast<T, TResult>(buffer);
        else
            result = ArenaAllocate<TResult>(ctx.Arena, buffer.Length);

        // TODO optimize here by choosing unchecked if values are within range
        for (int i = 0; i < buffer.Length; i++) result[i] = TResult.CreateChecked(buffer[i]);

        return result;
    }

    private static readonly int VectorSize = Vector<byte>.Count;

    public static bool All(BooleanArray array, Range range)
    {
        ReadOnlySpan<byte> bitmap = array.Values;

        (int Offset, int Length) offsetAndLength = range.GetOffsetAndLength(array.Length);
        int currentBit = offsetAndLength.Offset;
        int bitsRemaining = offsetAndLength.Length;

        if (bitsRemaining <= 0) return true;

        // 1. Handle Head (Unsynchronized bits up to the next byte boundary)
        int headBits = (8 - (currentBit & 7)) & 7;
        if (headBits > 0)
        {
            int bitsToRead = Math.Min(headBits, bitsRemaining);
            if (!MatchScalar(bitmap, currentBit, bitsToRead, true))
                return false;

            currentBit += bitsToRead;
            bitsRemaining -= bitsToRead;
        }

        if (bitsRemaining <= 0) return true;

        // Move to byte-based tracking
        int byteIndex = currentBit >> 3;
        int byteLength = bitsRemaining >> 3;
        int tailBits = bitsRemaining & 7;

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
        int currentBit = offsetAndLength.Offset;
        int bitsRemaining = offsetAndLength.Length;

        // 1. Handle Head
        int headBits = (8 - (currentBit & 7)) & 7;
        if (headBits > 0)
        {
            int bitsToRead = Math.Min(headBits, bitsRemaining);
            if (MatchScalar(bitmap, currentBit, bitsToRead, false))
                return true;

            currentBit += bitsToRead;
            bitsRemaining -= bitsToRead;
        }

        if (bitsRemaining <= 0) return false;

        int byteIndex = currentBit >> 3;
        int byteLength = bitsRemaining >> 3;
        int tailBits = bitsRemaining & 7;

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
        int byteIndex = bitOffset >> 3;
        int bitPos = bitOffset & 7;

        // Create a mask for the bits we care about in this byte
        // e.g., if bitPos = 2 and length = 3, we want bits 2, 3, and 4.
        byte mask = (byte)(((1 << length) - 1) << bitPos);
        byte value = (byte)(bitmap[byteIndex] & mask);

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
        int bitLength = mask.Length;
        int byteLength = (bitLength + 7) / 8;
        Span<byte> destination = ArenaAllocate<byte>(ctx.Arena, byteLength);

        int i = 0;
        int vecSize = Vector<T>.Count;

        if (Vector.IsHardwareAccelerated && mask.Length >= vecSize)
        {
            int vecEnd = mask.Length - mask.Length % vecSize;
            for (; i < vecEnd; i += vecSize)
            {
                Vector<T> vec = new(mask.Slice(i, vecSize));
                for (int j = 0; j < vecSize; j++)
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
            ExpressionType.GreaterThanOrEqual => Vector.GreaterThanOrEqual,
            ExpressionType.LessThanOrEqual => Vector.LessThanOrEqual,
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
