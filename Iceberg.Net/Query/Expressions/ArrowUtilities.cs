using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using DotNext.Linq.Expressions;
using Iceberg.Net.Misc;
using Iceberg.Net.Query.FastArrow;

namespace Iceberg.Net.Query.Expressions;

public static class ArrowUtilities
{
    public static StructArray AsStructArray(this RecordBatch batch)
    {
        return new StructArray(
            new StructType(batch.Schema.FieldsList),
            batch.Length,
            batch.Arrays,
            ArrowBuffer.Empty);
    }

    public static RecordBatch AsRecordBatch(this StructArray array, Schema schema)
    {
        return new RecordBatch(schema, array.Fields, array.Length);
    }

    public static ConstantExpression MakeConstantArray(Type elementType, object? val, int size)
    {
        MethodInfo method = typeof(ArrowUtilities)
            .GetMethod(
                nameof(MakeConstantArrayInternal),
                BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(elementType);
        return (ConstantExpression)method.Invoke(null, [val, size])!;
    }

    private static ConstantExpression MakeConstantArrayInternal<T>(T? item, int size) where T : struct, IEquatable<T>
    {
        if (item.HasValue)
        {
            ArrowBufferBuilder<T> builder = new(size);
            builder.Resize(size);
            builder.Span.Fill(item.Value);
            IdentityInput<PrimitiveArray<T>> constantInput = new(ArrayFromBuffer<T>(builder.Build(), size));
            return constantInput.Quoted;
        }
        else
        {
            throw new NotImplementedException("not yet");
        }
    }

    internal static ArrowTypeInfo ListOf(IArrowType elementType)
    {
        return new ArrowTypeInfo(new ListType(elementType), typeof(ListArray), typeof(ListArrayBuilder));
    }

    internal static ArrowTypeInfo ListViewOf(IArrowType elementType)
    {
        return new ArrowTypeInfo(new ListViewType(elementType), typeof(ListViewArray), typeof(ListViewArrayBuilder));
    }

    internal static ArrowTypeInfo GetTypeInfo(Type type)
    {
        if (type.ImplementsInterface(typeof(IEnumerable<>))) return ListOf(GetTypeInfo(type.GetGenericArguments()[0]).ArrowType);
        if (!TypeInfo.TryGetValue(type, out ArrowTypeInfo? info))
            throw new NotSupportedException($"The type {type.FullName} is not supported.");

        return info;
    }

    public static T AccessStructField<T>(StructArray arr, int index) where T : class, IArrowArray
    {
        // TODO use Unsafe.As in release mode
        return (T)arr.Fields[index];
    }


    private static PrimitiveArray<T> ArrayFromBuffer<T>(ArrowBuffer valueBuffer, int length)
        where T : struct, IEquatable<T>
    {
        PrimitiveArray<T>? result = (PrimitiveArray<T>)ArrowArrayFactory.BuildArray(
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
        ArrowBuffer.Builder<T> builder = new();
        builder.Append(span);
        PrimitiveArray<T>? result = (PrimitiveArray<T>)ArrowArrayFactory.BuildArray(
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
        BooleanArray? result = (BooleanArray)ArrowArrayFactory.BuildArray(
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
        ArrowBuffer.BitmapBuilder builder = new(length);
        if (bitmap.Length > 0)
            bitmap[..((length + 7) / 8)].CopyTo(builder.Span);
        return BooleanArrayFromBuffer(builder.Build(), length);
    }

    private static readonly IReadOnlyDictionary<Type, ArrowTypeInfo> TypeInfo = new Dictionary<Type, ArrowTypeInfo>
    {
        { typeof(int), new ArrowTypeInfo(Int32Type.Default, typeof(Int32Array), typeof(Int32Array.Builder)) },
        { typeof(double), new ArrowTypeInfo(DoubleType.Default, typeof(DoubleArray), typeof(DoubleArray.Builder)) },
        { typeof(bool), new ArrowTypeInfo(BooleanType.Default, typeof(BooleanArray), typeof(BooleanArrayBuilder)) }
    };

    internal record ArrowTypeInfo(IArrowType ArrowType, Type ArrayType, Type BuilderType);
}