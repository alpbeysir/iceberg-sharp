using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq.CompilerServices;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DotNext.Linq.Expressions;
using Iceberg.Net.Misc;
using Iceberg.Net.Schemas;
using Varena;
using ZLinq.Simd;
using Schema = Apache.Arrow.Schema;
using StructType = Apache.Arrow.Types.StructType;

namespace Iceberg.Net.Query.Expressions;

public sealed class ExecutionContext
{
    public required VirtualBuffer Arena { get; init; }
    public required VirtualArenaManager Manager { get; init; }
}

public interface IPrimitiveBuilder<T> where T : struct, IEquatable<T>
{
    public void Append(T value);

    public void Append(T? value);

    public void Append(ReadOnlySpan<T> span);

    public void AppendRange(IEnumerable<T> values);

    public void AppendNull();
}

public class PrimitiveBuilderWrapper<T, TArray, TBuilder> : IPrimitiveBuilder<T>
    where TBuilder : class, IArrowArrayBuilder<TArray>
    where TArray : IArrowArray
    where T : struct, IEquatable<T>
{
    private PrimitiveArrayBuilder<T, TArray, TBuilder> _builder;

    public void Append(T value)
    {
        _builder.Append(value);
    }

    public void Append(T? value)
    {
        _builder.Append(value);
    }

    public void Append(ReadOnlySpan<T> span)
    {
        _builder.Append(span);
    }

    public void AppendRange(IEnumerable<T> values)
    {
        _builder.AppendRange(values);
    }

    public void AppendNull()
    {
        _builder.AppendNull();
    }

    public static IPrimitiveBuilder<T> OfType()
    {
        throw new NotImplementedException();
    }
}

public static class ArrowExtensions
{
    public static StructArray AsStructArray(this RecordBatch batch)
    {
        return new StructArray(new StructType(batch.Schema.FieldsList), batch.Length, batch.Arrays, ArrowBuffer.Empty);
    }

    public static RecordBatch AsRecordBatch(this StructArray array, Schema schema)
    {
        return new RecordBatch(schema, array.Fields, array.Length);
    }

    public static StructArray MakeStructArray(StructType type, IArrowArray[] arrays)
    {
        // TODO account for constants
        var length = arrays[0].Length;
        return new StructArray(type, length, arrays, ArrowBuffer.Empty);
    }

    public static ConstantExpression MakeConstantArray(Type elementType, object? val, int size)
    {
        var method = typeof(ArrowExtensions)
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

    public static ArrowTypeInfo GetTypeInfo(Type type)
    {
        if (!TypeInfo.TryGetValue(type, out var info))
            throw new NotSupportedException($"The type {type.FullName} is not supported.");

        return info;
    }

    public static PrimitiveArray<T> ArrayFromBuffer<T>(ArrowBuffer valueBuffer, int length)
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

    public static BooleanArray BooleanArrayFromBuffer(ArrowBuffer valueBuffer, int length)
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

    private static readonly IReadOnlyDictionary<Type, ArrowTypeInfo> TypeInfo = new Dictionary<Type, ArrowTypeInfo>
    {
        { typeof(int), new ArrowTypeInfo(new Int32Type(), typeof(Int32Array), typeof(Int32Array.Builder)) },
        { typeof(double), new ArrowTypeInfo(new DoubleType(), typeof(DoubleArray), typeof(DoubleArray.Builder)) },
        { typeof(bool), new ArrowTypeInfo(new BooleanType(), typeof(BooleanArray), typeof(BooleanArray.Builder)) }
    };

    public record ArrowTypeInfo(IArrowType ArrowType, Type ArrayType, Type BuilderType);
}

public static class ArrowCompute
{
    // TODO this will not handle nulls properly
    public static PrimitiveArray<T> Zip<T>(
        ExecutionContext ctx,
        PrimitiveArray<T> l,
        PrimitiveArray<T> r,
        ExpressionType expressionType)
        where T : unmanaged, INumber<T>
    {
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
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
        var zip = l.Values.AsVectorizable().Zip(
            r.Values,
            vectorSelector,
            selector);
        var builder = new ArrowBuffer.Builder<T>(l.Length);
        builder.Resize(l.Length);
        zip.CopyTo(builder.Span);
        return ArrowExtensions.ArrayFromBuffer<T>(builder.Build(), l.Length);
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

    public static BooleanArray BitmapOps(
        ExecutionContext ctx,
        BooleanArray l,
        BooleanArray r,
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
            ExpressionType.And => (n1, n2) => (byte)(n1 & n2),
            ExpressionType.AndAlso => (n1, n2) => (byte)(n1 & n2),
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
        var zip = l.Values.AsVectorizable().Zip(
            r.Values,
            vectorSelector,
            selector);
        var builder = new ArrowBuffer.BitmapBuilder();
        builder.Resize(l.Length);
        zip.CopyTo(builder.Span);
        return ArrowExtensions.BooleanArrayFromBuffer(builder.Build(), l.Length);
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
    
    // public static PrimitiveArray<TResult> Select<T, TResult>(
    //     ExecutionContext ctx,
    //     ReadOnlySpan<T> span,
    //     ExpressionType expressionType)
    //     where T : struct, INumber<T>
    //     where TResult : struct, INumber<TResult>
    // {
    //     // TODO precompute these switches
    //     Func<Vector<T>, Vector<TResult>> vectorSelector = expressionType switch
    //     {
    //         ExpressionType.Convert => VectorCastHelper<T, TResult>,
    //         _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
    //     };
    //     Func<T, TResult> selector = expressionType switch
    //     {
    //         ExpressionType.Convert => Unsafe.BitCast<T, TResult>,
    //         _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
    //     };
    //     var select = span.AsVectorizable().Select(
    //         vectorSelector,
    //         selector);
    //     var builder = new ArrowBuffer.Builder<TResult>(span.Length);
    //     builder.Resize(span.Length);
    //     select.CopyTo(builder.Span);
    //     return FromBuffer<TResult>(builder.Build(), span.Length);
    // }

    public static PrimitiveArray<TResult> ConvertLogical<T, TResult>(
        ExecutionContext ctx,
        PrimitiveArray<T> buffer)
        where T : struct, INumber<T>
        where TResult : struct, INumber<TResult>
    {
        var sourceSpan = buffer.Values;
        var builder = new ArrowBuffer.Builder<TResult>(sourceSpan.Length);
        builder.Resize(sourceSpan.Length);
        var resultSpan = builder.Span;
        for (var i = 0; i < sourceSpan.Length; i++) resultSpan[i] = TResult.CreateChecked(sourceSpan[i]);

        return ArrowExtensions.ArrayFromBuffer<TResult>(builder.Build(), resultSpan.Length);
    }

    public static BooleanArray BooleanArrayFromMask<T>(ExecutionContext ctx, PrimitiveArray<T> mask)
        where T : struct, INumber<T>
    {
        var source = mask.Values;

        var builder = new ArrowBuffer.BitmapBuilder();
        builder.Resize(mask.Length);
        var destination = builder.Span;

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

        return ArrowExtensions.BooleanArrayFromBuffer(builder.Build(), mask.Length);
    }

    private static ReadOnlySpan<T> SpanFromBuffer<T>(ArrowBuffer buffer)
        where T : struct
    {
        var sourceSpan = buffer.Span.CastTo<T>()[..buffer.Length];
        return sourceSpan;
    }
}

public class BufferTransformVisitor : ExpressionVisitorNarrow<Expression, LambdaExpression, Expression,
    NewExpression, ElementInit, MemberBinding, MemberAssignment, MemberListBinding, MemberMemberBinding>
{
    private readonly Dictionary<string, ParameterExpression> _bindings = [];
    private readonly ParameterExpression _ctxParam = Expression.Parameter(typeof(ExecutionContext), "ctx");
    private readonly Stack<ParameterExpression> _indexVars = [];
    private readonly Dictionary<MemberInfo, int> _memberIndex = [];
    private readonly int MaxBatchSize = 65536;
    private QueryStepVisitor QueryStepVisitor => new([]);

    protected override Expression MakeBinary(
        BinaryExpression node,
        Expression left,
        LambdaExpression conversion,
        Expression right)
    {
        var leftElementType = BufferElementType(left);
        var rightElementType = BufferElementType(right);

        // if not equal, convert both to bitmap
        if (leftElementType != rightElementType)
        {
            if (leftElementType != typeof(bool))
            {
                var leftBitmapFromMaskMethod =
                    typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BooleanArrayFromMask))!.MakeGenericMethod(
                        leftElementType);
                left = Expression.Call(
                    null,
                    leftBitmapFromMaskMethod,
                    _ctxParam,
                    left);
            }

            if (rightElementType != typeof(bool))
            {
                var rightBitmapFromMaskMethod =
                    typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BooleanArrayFromMask))!
                        .MakeGenericMethod(rightElementType);
                right = Expression.Call(
                    null,
                    rightBitmapFromMaskMethod,
                    _ctxParam,
                    right);
            }

            var bitmapOpsMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.BitmapOps))!;
            var bitmapOpsCall = Expression.Call(
                null,
                bitmapOpsMethod,
                _ctxParam,
                left,
                right,
                node.NodeType.Quoted);
            return bitmapOpsCall;
        }

        var zipMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Zip))!.MakeGenericMethod(leftElementType);
        var zipCall = Expression.Call(
            null,
            zipMethod,
            _ctxParam,
            left,
            right,
            node.NodeType.Quoted);
        return zipCall;
    }

    protected override Expression MakeConditional(
        ConditionalExpression node,
        Expression test,
        Expression ifTrue,
        Expression ifFalse)
    {
        return node;
    }

    protected override Expression MakeConstant(ConstantExpression node)
    {
        return ArrowExtensions.MakeConstantArray(node.Type, node.Value, MaxBatchSize);
    }

    protected override Expression MakeDefault(DefaultExpression node)
    {
        return node;
    }

    protected override ElementInit MakeElementInit(ElementInit node, ReadOnlyCollection<Expression> arguments)
    {
        return node;
    }

    protected override Expression MakeInvocation(
        InvocationExpression node,
        Expression expression,
        ReadOnlyCollection<Expression> arguments)
    {
        return node;
    }

    protected override LambdaExpression MakeLambda<T>(
        Expression<T> node,
        Expression body,
        ReadOnlyCollection<Expression> parameters)
    {
        return Expression.Lambda(body, [_ctxParam, .._bindings.Values]);
    }

    protected override Expression MakeListInit(
        ListInitExpression node,
        NewExpression newExpression,
        ReadOnlyCollection<ElementInit> initializers)
    {
        return node;
    }

    protected override Expression MakeMember(MemberExpression node, Expression buffer)
    {
        return AccessCompositeBuffer(node, buffer);
    }

    protected override MemberAssignment MakeMemberAssignment(MemberAssignment node, Expression expression)
    {
        return node;
    }

    protected override Expression MakeMemberInit(
        MemberInitExpression node,
        NewExpression newExpression,
        ReadOnlyCollection<MemberBinding> bindings)
    {
        return node;
    }

    protected override MemberListBinding MakeMemberListBinding(
        MemberListBinding node,
        ReadOnlyCollection<ElementInit> initializers)
    {
        return node;
    }

    protected override MemberMemberBinding MakeMemberMemberBinding(
        MemberMemberBinding node,
        ReadOnlyCollection<MemberBinding> bindings)
    {
        return node;
    }

    protected override Expression MakeMethodCall(
        MethodCallExpression node,
        Expression @object,
        ReadOnlyCollection<Expression> arguments)
    {
        return node;
    }

    protected override Expression MakeNew(NewExpression node, ReadOnlyCollection<Expression> arguments)
    {
        var method = typeof(ArrowExtensions).GetMethod(nameof(ArrowExtensions.MakeStructArray))!;
        var structType = new StructType(
            ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(node.Type, null, s => -1)).FieldsList);
        return Expression.Call(
            null,
            method,
            [structType.Quoted, Expression.NewArrayInit(typeof(IArrowArray), arguments)]);
    }

    protected override Expression MakeNewArray(NewArrayExpression node, ReadOnlyCollection<Expression> expressions)
    {
        return node;
    }

    protected override Expression MakeParameter(ParameterExpression node)
    {
        if (!_bindings.TryGetValue(node.Name!, out var param))
        {
            param = Expression.Parameter(GetBufferType(node.Type), node.Name);
            _bindings.Add(node.Name!, param);
        }

        if (param.Type.Name.Contains("PrimitiveArray")) return param;

        if (param.Type.Name.Contains("StructArray")) return param;

        throw new NotImplementedException("not yet");
    }

    protected override Expression MakeTypeBinary(TypeBinaryExpression node, Expression expression)
    {
        return node;
    }

    protected override Expression MakeUnary(UnaryExpression node, Expression operand)
    {
        if (node.NodeType == ExpressionType.Convert)
        {
            var convertMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))
                .MakeGenericMethod(node.Operand.Type, node.Type);
            return Expression.Call(
                null,
                convertMethod,
                [_ctxParam, operand]);
        }

        return node;
    }

    private Expression AccessCompositeBuffer(
        MemberExpression expr,
        Expression buffer)
    {
        if (buffer.Type == typeof(StructArray))
        {
            var method =
                typeof(BufferTransformVisitor).GetMethod(nameof(AccessField))!.MakeGenericMethod(
                    GetBufferType(expr.Type));
            return Expression.Call(null, method, buffer, _memberIndex[expr.Member].Quoted);
        }

        throw new InvalidOperationException("only struct can be accessed");
    }

    private Type GetBufferType(Type type)
    {
        if (type == typeof(int)) return typeof(PrimitiveArray<int>);
        if (type == typeof(double)) return typeof(PrimitiveArray<double>);

        if (type.ImplementsInterface(typeof(IReadOnlyDictionary<,>)))
        {
            var genericArguments = type.GetGenericArguments();
            var keyType = genericArguments[0];
            var valueType = genericArguments[1];
            var maybeUnderlyingType = Nullable.GetUnderlyingType(valueType);
            var valueRequired = maybeUnderlyingType == null;
            throw new NotImplementedException("not yet");
        }

        if (type.ImplementsInterface(typeof(IReadOnlyList<>)))
        {
            var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                var maybeUnderlyingType = Nullable.GetUnderlyingType(elementType);
                var required = maybeUnderlyingType == null;
                throw new NotImplementedException("not yet");
            }
        }

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
        {
            var members = type.GetMembers(BindingFlags.Instance | BindingFlags.Public)
                .Where(info => info is PropertyInfo or FieldInfo).ToList();
            foreach (var (idx, member) in members.Index()) _memberIndex.Add(member, idx);
            return typeof(StructArray);
        }

        throw new NotImplementedException("not yet");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T AccessField<T>(StructArray arr, int index) where T : class, IArrowArray
    {
        // TOOD use Unsafe.As in release mode
        return (T)arr.Fields[index];
    }

    private static Span<T> ArenaAllocateHelper<T>(VirtualBuffer buffer, int amount) where T : struct
    {
        return MemoryMarshal.Cast<byte, T>(buffer.AllocateRange(Unsafe.SizeOf<T>() * amount));
    }

    private static Type BufferElementType(Expression expression)
    {
        if (expression.Type == typeof(BooleanArray))
            return typeof(bool);

        if (expression.Type.ImplementsInterface(typeof(IArrowArray)))
            return expression.Type.GetGenericArguments()[0];

        throw new InvalidOperationException("invalid");
    }
}