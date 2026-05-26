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
        var zip = l.AsVectorizable().Zip(
            r,
            vectorSelector,
            selector);
        var result = ArenaAllocate<T>(ctx.Arena, l.Length);
        zip.CopyTo(result);
        return result;
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

    public static TResultBuilder ExecuteElementWiseListOp<TElementArray, TResultBuilder>(
        ExecutionContext ctx,
        ListArray l,
        TResultBuilder builder,
        Action<ExecutionContext, TElementArray, TResultBuilder> op)
        where TResultBuilder : IArrowArrayBuilder
        where TElementArray : IArrowArray
    {
        var asListBuilder = builder as ListArrayBuilder;
        for (var i = 0; i < l.Length; i++)
        {
            asListBuilder?.Append();
            var element = (TElementArray)l.GetSlicedValues(i);
            op(ctx, element, builder);
        }

        return builder;
    }

    // fast path when result is a list and we can reuse the original offsets
    public static ListArrayBuilder ExecuteOneToOneListOp<TElementArray>(
        ExecutionContext ctx,
        ListArray l,
        ListArrayBuilder builder,
        Action<ExecutionContext, TElementArray, ListArrayBuilder> op)
    {
        var values = (TElementArray)l.Values;
        op(ctx, values, builder);
        builder.InitializeFromList(l);
        return builder;
    }

    public static TResultBuilder MakeBuilderForGeneric<TResultBuilder>(IArrowType arrowType, MemoryAllocator? allocator)
        where TResultBuilder : IArrowArrayBuilder
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
            ListType l => new ListArrayBuilder(l.ValueDataType),
            StructType s => new StructArrayBuilder(s),
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
        var zip = l.AsVectorizable().Zip(
            r,
            vectorSelector,
            selector);
        var result = ArenaAllocate<byte>(ctx.Arena, l.Length);
        zip.CopyTo(result);
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T FromBoolMask<T>(bool value) where T : struct, INumber<T>
    {
        // If you need a SIMD-style mask (all bits set for true), 
        // we subtract 1 from 0 (results in -1, or all bits set in two's complement).
        var val = value ? T.One : T.Zero;

        // This creates -1 for true (0xFF...) and 0 for false (0x00...)
        return T.Zero - val;
    }

    private static Span<T> ArenaAllocate<T>(VirtualBuffer buffer, int amount) where T : struct
    {
        return MemoryMarshal.Cast<byte, T>(buffer.AllocateRange(Unsafe.SizeOf<T>() * amount));
    }

    public static ReadOnlySpan<TResult> ConvertLogical<T, TResult>(
        ExecutionContext ctx,
        ReadOnlySpan<T> buffer)
        where T : struct, INumber<T>
        where TResult : struct, INumber<TResult>
    {
        var result = ArenaAllocate<TResult>(ctx.Arena, buffer.Length);
        for (var i = 0; i < buffer.Length; i++) result[i] = TResult.CreateChecked(buffer[i]);

        return result;
    }

    public static ReadOnlySpan<byte> BooleanArrayFromMask<T>(ExecutionContext ctx, ReadOnlySpan<T> mask)
        where T : struct, INumber<T>
    {
        var source = mask;
        var destination = ArenaAllocate<byte>(ctx.Arena, mask.Length);

        var vectorSize = Vector<T>.Count;

        for (var i = 0; i <= source.Length - vectorSize; i += vectorSize)
        {
            // Ensure destination is large enough
            var bytesNeeded = (i + vectorSize + 7) / 8;
            if (destination.Length < bytesNeeded)
                throw new ArgumentException("Destination span is too small.");

            var vec = new Vector<T>(source[i..(i + vectorSize)]);

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