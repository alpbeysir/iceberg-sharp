using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using DotNext.Linq.Expressions;

namespace Iceberg.Net.Query.Expressions;

public static class ArrowUtilities
{
    public static StructArray AsStructArray(this RecordBatch batch)
    {
        return new StructArray(new StructType(batch.Schema.FieldsList), batch.Length, batch.Arrays, ArrowBuffer.Empty);
    }

    public static RecordBatch AsRecordBatch(this StructArray array, Schema schema)
    {
        return new RecordBatch(schema, array.Fields, array.Length);
    }

    public static StructArray MakeStructArray(this StructType type, IArrowArray[] arrays)
    {
        // TODO account for constants
        var length = arrays[0].Length;
        return new StructArray(type, length, arrays, ArrowBuffer.Empty);
    }

    public static ConstantExpression MakeConstantArray(Type elementType, object? val, int size)
    {
        var method = typeof(ArrowUtilities)
            .GetMethod(
                nameof(MakeConstantArrayInternal),
                BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(elementType);
        return (ConstantExpression)method.Invoke(null, [val, size])!;
    }

    private static ConstantExpression MakeConstantArrayInternal<T>(T? item, int size) where T : struct, IEquatable<T>
    {
        if (item.HasValue)
        {
            var builder = new ArrowBuffer.Builder<T>(size);
            builder.Resize(size);
            builder.Span.Fill(item.Value);
            return ArrayFromBuffer<T>(builder.Build(), size).Quoted;
        }
        else
        {
            throw new NotImplementedException("not yet");
        }
    }

    internal static ArrowTypeInfo GetTypeInfo(Type type)
    {
        if (!TypeInfo.TryGetValue(type, out var info))
            throw new NotSupportedException($"The type {type.FullName} is not supported.");

        return info;
    }

    private static PrimitiveArray<T> ArrayFromBuffer<T>(ArrowBuffer valueBuffer, int length)
        where T : struct, IEquatable<T>
    {
        var result = (PrimitiveArray<T>)ArrowArrayFactory.BuildArray(
            new ArrayData(
                GetTypeInfo(typeof(T)).ArrowType,
                length,
                0,
                0,
                [ArrowBuffer.Empty, valueBuffer],
                []
            )
        );
        return result;
    }

    public static PrimitiveArray<T> ArrayFromSpan<T>(ReadOnlySpan<T> span)
        where T : struct, IEquatable<T>
    {
        var builder = new ArrowBuffer.Builder<T>();
        builder.Append(span);
        var result = (PrimitiveArray<T>)ArrowArrayFactory.BuildArray(
            new ArrayData(
                GetTypeInfo(typeof(T)).ArrowType,
                span.Length,
                0,
                0,
                [ArrowBuffer.Empty, builder.Build()],
                []
            )
        );
        return result;
    }

    private static BooleanArray BooleanArrayFromBuffer(ArrowBuffer valueBuffer, int length)
    {
        var result = (BooleanArray)ArrowArrayFactory.BuildArray(
            new ArrayData(
                GetTypeInfo(typeof(bool)).ArrowType,
                length,
                0,
                0,
                [ArrowBuffer.Empty, valueBuffer],
                []
            )
        );
        return result;
    }

    public static BooleanArray BooleanArrayFromBitmap(ReadOnlySpan<byte> bitmap, int length)
    {
        var builder = new ArrowBuffer.BitmapBuilder(length);
        if (bitmap.Length > 0)
            bitmap[..((length + 7) / 8)].CopyTo(builder.Span);
        return BooleanArrayFromBuffer(builder.Build(), length);
    }

    private static readonly IReadOnlyDictionary<Type, ArrowTypeInfo> TypeInfo = new Dictionary<Type, ArrowTypeInfo>
    {
        { typeof(int), new ArrowTypeInfo(new Int32Type(), typeof(Int32Array), typeof(Int32Array.Builder)) },
        { typeof(double), new ArrowTypeInfo(new DoubleType(), typeof(DoubleArray), typeof(DoubleArray.Builder)) },
        { typeof(bool), new ArrowTypeInfo(new BooleanType(), typeof(BooleanArray), typeof(BooleanArray.Builder)) }
    };

    internal record ArrowTypeInfo(IArrowType ArrowType, Type ArrayType, Type BuilderType);
}