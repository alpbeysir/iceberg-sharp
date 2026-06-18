using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DotNext.Linq.Expressions;
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
            IdentityInput constantInput = new(ArrayFromBuffer<T>(builder.Build(), size));
            return constantInput.Quoted;
        }
        else
        {
            throw new NotImplementedException("not yet");
        }
    }

    public static T AccessStructField<T>(StructArray arr, int index) where T : class, IArrowArray
    {
#if DEBUG
        return (T)arr.FastFields()[index];
#else
        return Unsafe.As<T>(arr.FastFields()[index]);
#endif
    }


    private static PrimitiveArray<T> ArrayFromBuffer<T>(ArrowBuffer valueBuffer, int length)
        where T : struct, IEquatable<T>
    {
        PrimitiveArray<T>? result = (PrimitiveArray<T>)ArrowArrayFactory.BuildArray(
            new ArrayData(
                ArrowTypeUtils.ForCSharpType(typeof(T)).ArrowType,
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
                ArrowTypeUtils.ForCSharpType(typeof(T)).ArrowType,
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
                ArrowTypeUtils.ForCSharpType(typeof(bool)).ArrowType,
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

}