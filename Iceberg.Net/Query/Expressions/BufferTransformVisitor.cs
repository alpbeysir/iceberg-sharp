using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq.CompilerServices;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using DotNext.Linq.Expressions;
using Iceberg.Net.Misc;
using Iceberg.Net.Schemas;
using Varena;
using ZLinq.Simd;
using ListType = Apache.Arrow.Types.ListType;
using Schema = Apache.Arrow.Schema;
using StructType = Apache.Arrow.Types.StructType;

namespace Iceberg.Net.Query.Expressions;

public sealed class ExecutionContext
{
    public required VirtualBuffer Arena { get; init; }
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

    public static TResultArray ExecuteListOp<TElementArray, TResultArray, TResultBuilder>(
        ExecutionContext ctx,
        IArrowType resultType,
        ListArray l,
        Action<ExecutionContext, TElementArray, IArrowArrayBuilder<TResultArray, TResultBuilder>> op)
        where TResultBuilder : IArrowArrayBuilder<TResultArray, TResultBuilder>
        where TResultArray : IArrowArray
    {
        var builder = MakeBuilderFor<TResultArray, TResultBuilder>(resultType);
        for (var i = 0; i < l.Length; i++)
        {
            if (builder is ListArray.Builder lb) lb.Append();
            var element = (TElementArray)l.GetSlicedValues(i);
            op(ctx, element, builder);
        }

        return builder.Build(MemoryAllocator.Default.Value);
    }

    public static IArrowArrayBuilder<TResultArray, TResultBuilder>
        MakeBuilderFor<TResultArray, TResultBuilder>(IArrowType arrowType)
        where TResultArray : IArrowArray
        where TResultBuilder : IArrowArrayBuilder<TResultArray>
    {
        return (IArrowArrayBuilder<TResultArray, TResultBuilder>)MakeBuilderFor(arrowType);
    }

    public static IArrowArrayBuilder MakeBuilderFor(IArrowType arrowType)
    {
        return arrowType switch
        {
            DoubleType => new DoubleArray.Builder(),
            Int32Type => new Int32Array.Builder(),
            BooleanType => new BooleanArray.Builder(),
            ListType l => new ListArray.Builder(l.ValueDataType),
            StructType s => new StructArrayBuilder(s),
            _ => throw new ArgumentOutOfRangeException(nameof(arrowType), arrowType, null)
        };
    }

    // TODO don't pass in interface here, use generics to avoid
    public static void AppendToBuilder<T>(IArrowArrayBuilder builder, ReadOnlySpan<T> values)
        where T : struct, IEquatable<T>
    {
        if (typeof(T) == typeof(int))
            ((Int32Array.Builder)builder).Append(MemoryMarshal.Cast<T, int>(values));
        else if (typeof(T) == typeof(double))
            ((DoubleArray.Builder)builder).Append(MemoryMarshal.Cast<T, double>(values));
        else if (typeof(T) == typeof(bool))
            ((BooleanArray.Builder)builder).Append(MemoryMarshal.Cast<T, bool>(values));
        else
            throw new NotSupportedException(
                $"Type {typeof(T).FullName} is not supported for bulk append. Use AppendArrayToBuilder instead.");
    }

    public static void AppendToListBuilder<T>(
        IArrowArrayBuilder<ListArray, ListArray.Builder> listBuilder,
        ReadOnlySpan<T> values)
        where T : struct, IEquatable<T>
    {
        var lb = (ListArray.Builder)listBuilder;
        AppendToBuilder(lb.ValueBuilder, values);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AllTrue<T>(ReadOnlySpan<T> mask)
        where T : unmanaged, INumber<T>
    {
        foreach (var t in mask)
            if (t == T.Zero)
                return false;

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AnyTrue<T>(ReadOnlySpan<T> mask)
        where T : unmanaged, INumber<T>
    {
        foreach (var t in mask)
            if (t != T.Zero)
                return true;

        return false;
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

public class BufferTransformVisitor : ExpressionVisitorNarrow<Expression, LambdaExpression, Expression,
    NewExpression, ElementInit, MemberBinding, MemberAssignment, MemberListBinding, MemberMemberBinding>
{
    private readonly Dictionary<string, ParameterExpression> _bindings = [];
    private readonly ParameterExpression _ctxParam = Expression.Parameter(typeof(ExecutionContext), "ctx");
    private readonly Dictionary<MemberInfo, int> _memberIndex = [];
    private readonly Stack<Expression> _builderStack = new();
    private const int MaxBatchSize = 65536;

    private Expression GenerateListSelect(Expression source, LambdaExpression transformedInner)
    {
        // transformedInner: (ctx, elementArray) => ReadOnlySpan<TResult>
        var elementArrayType = transformedInner.Parameters[1].Type;
        var resultElementType = transformedInner.ReturnType.GetGenericArguments()[0];

        var valueType = ArrowExtensions.GetTypeInfo(resultElementType).ArrowType;
        var listType = new ListType(valueType);

        var selectMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListOp))!
            .MakeGenericMethod(elementArrayType, typeof(ListArray), typeof(ListArray.Builder));

        var ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        var elemP = Expression.Parameter(elementArrayType, "elem");
        var builderP = Expression.Parameter(
            typeof(IArrowArrayBuilder<ListArray, ListArray.Builder>),
            "builder");

        var invokeInner = Expression.Invoke(transformedInner, ctxP, elemP);
        var appendMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AppendToListBuilder))!
            .MakeGenericMethod(resultElementType);
        var appendCall = Expression.Call(null, appendMethod, builderP, invokeInner);

        var opLambda = Expression.Lambda(appendCall, ctxP, elemP, builderP);

        return Expression.Call(null, selectMethod, _ctxParam, listType.Quoted, source, opLambda);
    }

    private Expression GenerateListAll(Expression source, LambdaExpression transformedInner)
    {
        // transformedInner: (ctx, elementArray) => ReadOnlySpan<T> (mask)
        var elementArrayType = transformedInner.Parameters[1].Type;
        var resultElementType = transformedInner.ReturnType.GetGenericArguments()[0];

        var selectMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListOp))!
            .MakeGenericMethod(elementArrayType, typeof(BooleanArray), typeof(BooleanArray.Builder));

        var ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        var elemP = Expression.Parameter(elementArrayType, "elem");
        var builderP = Expression.Parameter(
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>),
            "builder");

        var invokeInner = Expression.Invoke(transformedInner, ctxP, elemP);
        var allTrueMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AllTrue))!
            .MakeGenericMethod(resultElementType);
        var allTrueCall = Expression.Call(null, allTrueMethod, invokeInner);

        var appendCall = Expression.Call(
            Expression.Convert(builderP, typeof(BooleanArray.Builder)),
            typeof(BooleanArray.Builder).GetMethod("Append", [typeof(bool)])!,
            allTrueCall);

        var actionType = typeof(Action<,,>).MakeGenericType(
            typeof(ExecutionContext),
            elementArrayType,
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>));
        var opLambda = Expression.Lambda(
            actionType,
            Expression.Block(appendCall, Expression.Empty()),
            ctxP,
            elemP,
            builderP);

        return Expression.Call(
            null,
            selectMethod,
            _ctxParam,
            BooleanType.Default.Quoted,
            source,
            opLambda);
    }

    private Expression GenerateListAny(Expression source, LambdaExpression transformedInner)
    {
        var elementArrayType = transformedInner.Parameters[1].Type;
        var resultElementType = transformedInner.ReturnType.GetGenericArguments()[0];

        var selectMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListOp))!
            .MakeGenericMethod(elementArrayType, typeof(BooleanArray), typeof(BooleanArray.Builder));

        var ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        var elemP = Expression.Parameter(elementArrayType, "elem");
        var builderP = Expression.Parameter(
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>),
            "builder");

        var invokeInner = Expression.Invoke(transformedInner, ctxP, elemP);
        var anyTrueMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AnyTrue))!
            .MakeGenericMethod(resultElementType);
        var anyTrueCall = Expression.Call(null, anyTrueMethod, invokeInner);

        var appendCall = Expression.Call(
            Expression.Convert(builderP, typeof(BooleanArray.Builder)),
            typeof(BooleanArray.Builder).GetMethod("Append", [typeof(bool)])!,
            anyTrueCall);

        var actionType = typeof(Action<,,>).MakeGenericType(
            typeof(ExecutionContext),
            elementArrayType,
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>));
        var opLambda = Expression.Lambda(
            actionType,
            Expression.Block(appendCall, Expression.Empty()),
            ctxP,
            elemP,
            builderP);

        return Expression.Call(
            null,
            selectMethod,
            _ctxParam,
            BooleanType.Default.Quoted,
            source,
            opLambda);
    }

    private Expression GenerateListContains(Expression source, Expression valueExpr)
    {
        if (valueExpr is not ConstantExpression ce)
            return Expression.Empty(); // TODO

        var elementType = Nullable.GetUnderlyingType(valueExpr.Type) ?? valueExpr.Type;
        var elementArrayType = typeof(PrimitiveArray<>).MakeGenericType(elementType);

        var selectMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ExecuteListOp))!
            .MakeGenericMethod(elementArrayType, typeof(BooleanArray), typeof(BooleanArray.Builder));

        var ctxP = Expression.Parameter(typeof(ExecutionContext), "ctx");
        var elemP = Expression.Parameter(elementArrayType, "elem");
        var builderP = Expression.Parameter(
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>),
            "builder");

        var containsMethod = typeof(MemoryExtensions).GetMethod(
            "Contains",
            [typeof(ReadOnlySpan<>).MakeGenericType(elementType), elementType])!;
        var valuesProp = Expression.Property(elemP, "Values");
        var containsCall = Expression.Call(
            null,
            containsMethod,
            valuesProp,
            Expression.Constant(ce.Value, elementType));

        var appendCall = Expression.Call(
            Expression.Convert(builderP, typeof(BooleanArray.Builder)),
            typeof(BooleanArray.Builder).GetMethod("Append", [typeof(bool)])!,
            containsCall);

        var actionType = typeof(Action<,,>).MakeGenericType(
            typeof(ExecutionContext),
            elementArrayType,
            typeof(IArrowArrayBuilder<BooleanArray, BooleanArray.Builder>));
        var opLambda = Expression.Lambda(
            actionType,
            Expression.Block(appendCall, Expression.Empty()),
            ctxP,
            elemP,
            builderP);

        return Expression.Call(
            null,
            selectMethod,
            _ctxParam,
            BooleanType.Default.Quoted,
            source,
            opLambda);
    }

    protected override Expression MakeBinary(
        BinaryExpression node,
        Expression left,
        LambdaExpression conversion,
        Expression right)
    {
        var leftElementType = PrimitiveBufferElementType(left);
        var rightElementType = PrimitiveBufferElementType(right);

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
            AccessValues(left),
            AccessValues(right),
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

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        var returnType = Nullable.GetUnderlyingType(node.ReturnType) ?? node.ReturnType;
        var builderParam = Expression.Parameter(
            GetConcreteBuilderType(GetBufferType(returnType)),
            "builder");
        _builderStack.Push(builderParam);

        var result = base.VisitLambda(node);
        _builderStack.Pop();
        return result;
    }

    private static Type GetConcreteBuilderType(Type bufferType)
    {
        if (bufferType.IsGenericType && bufferType.GetGenericTypeDefinition() == typeof(PrimitiveArray<>))
        {
            var innerType = bufferType.GetGenericArguments()[0];
            return ArrowExtensions.GetTypeInfo(innerType).BuilderType;
        }

        if (bufferType == typeof(StructArray)) return typeof(StructArrayBuilder);
        if (bufferType == typeof(ListArray)) return typeof(ListArray.Builder);
        if (bufferType == typeof(BooleanArray)) return typeof(BooleanArray.Builder);
        throw new NotSupportedException($"No concrete builder for {bufferType.Name}");
    }

    protected override LambdaExpression MakeLambda<T>(
        Expression<T> node,
        Expression body,
        ReadOnlyCollection<Expression> parameters)
    {
        List<ParameterExpression> lambdaParams = [_ctxParam];
        foreach (var expression in parameters)
        {
            var p = (ParameterExpression)expression;
            if (_bindings.TryGetValue(p.Name!, out var arrowParam))
                lambdaParams.Add(arrowParam);
        }

        if (_builderStack.Count > 0 && _builderStack.Peek() is ParameterExpression builderParam)
            lambdaParams.Add(builderParam);

        if (_builderStack.Count > 0 && body.Type != typeof(void))
        {
            var builder = _builderStack.Peek();
            if (body.Type.Name.Contains("ReadOnlySpan"))
                body = Expression.Call(
                    null,
                    typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.AppendToBuilder))!
                        .MakeGenericMethod(body.Type.GetGenericArguments()[0]),
                    Expression.Convert(builder, typeof(IArrowArrayBuilder)),
                    body);
            else
                body = Expression.Block(body, Expression.Empty());
        }

        var lambda = Expression.Lambda(body, lambdaParams);

        foreach (var expression in parameters)
        {
            var p = (ParameterExpression)expression;
            _bindings.Remove(p.Name!);
        }

        return lambda;
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
        var declaringType = node.Method.DeclaringType;
        if (declaringType == null ||
            (!declaringType.Name.Contains("Enumerable") && !declaringType.Name.Contains("Queryable")))
            return node;

        var methodName = node.Method.Name;
        if (methodName is not ("Select" or "All" or "Any" or "Contains"))
            return node;

        var source = arguments[0];
        if (source.Type != typeof(ListArray) && source.Type != typeof(LargeListArray))
            return node;

        if (methodName == "Contains")
            return GenerateListContains(source, node.Arguments[1]);

        // already visited by base — MakeLambda scoped bindings correctly
        var lambdaArg = arguments[1];
        if (lambdaArg is UnaryExpression { NodeType: ExpressionType.Quote } u)
            lambdaArg = u.Operand;

        if (lambdaArg is not LambdaExpression transformedInner)
            return node;

        return methodName switch
        {
            "Select" => GenerateListSelect(source, transformedInner),
            "All" => GenerateListAll(source, transformedInner),
            "Any" => GenerateListAny(source, transformedInner),
            _ => node
        };
    }

    protected override Expression VisitNew(NewExpression node)
    {
        var type = node.Type;
        if (_builderStack.Count == 0
            || !(type.IsClass || type is { IsValueType: true, IsPrimitive: false }))
            return base.VisitNew(node);

        var structType = new StructType(
            ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(node.Type, null, _ => -1)).FieldsList);
        var members = type.GetMembers(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m is PropertyInfo or FieldInfo).ToList();

        var visitedArgs = new List<Expression>(node.Arguments.Count);
        for (var i = 0; i < node.Arguments.Count; i++)
        {
            var memberType = Utils.PropertyOrFieldType(members[i]);
            var underlying = Nullable.GetUnderlyingType(memberType) ?? memberType;

            if (underlying.IsClass || underlying is { IsValueType: true, IsPrimitive: false })
            {
                var parentBuilder = _builderStack.Peek();
                var sbExpr = Expression.Convert(parentBuilder, typeof(StructArrayBuilder));
                var subBuilder = Expression.Call(sbExpr,
                    typeof(StructArrayBuilder).GetMethod(
                        nameof(StructArrayBuilder.GetFieldBuilder))!
                        .MakeGenericMethod(typeof(StructArrayBuilder)),
                    Expression.Constant(i));
                _builderStack.Push(subBuilder);
                visitedArgs.Add(Visit(node.Arguments[i]));
                _builderStack.Pop();
            }
            else
            {
                visitedArgs.Add(Visit(node.Arguments[i]));
            }
        }

        return MakeNew(node, visitedArgs.AsReadOnly());
    }

    protected override Expression MakeNew(NewExpression node, ReadOnlyCollection<Expression> arguments)
    {
        var type = node.Type;

        if (type.ImplementsInterface(typeof(IReadOnlyDictionary<,>)))
        {
            throw new NotImplementedException("not yet");
        }

        if (type.ImplementsInterface(typeof(IEnumerable<>)))
        {
            var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                throw new NotImplementedException("not yet");
            }
        }

        if (type.IsClass || type is { IsValueType: true, IsPrimitive: false })
        {
            var structType = new StructType(
                ArrowSchema.FromSchema(CSharpSchema.ToIcebergSchema(node.Type, null, _ => -1)).FieldsList);

            if (_builderStack.Count > 0)
                return BuildStructWithBuilder(structType, arguments);

            var method = typeof(ArrowExtensions).GetMethod(nameof(ArrowExtensions.MakeStructArray))!;
            return Expression.Call(
                null,
                method,
                [
                    structType.Quoted,
                    Expression.NewArrayInit(typeof(IArrowArray), arguments.Select(MakeBuffer))
                ]);
        }

        throw new NotImplementedException("not yet");
    }

    private Expression BuildStructWithBuilder(
        StructType structType,
        ReadOnlyCollection<Expression> arguments)
    {
        var builder = _builderStack.Peek();
        var sbExpr = Expression.Convert(builder, typeof(StructArrayBuilder));
        var body = new List<Expression>(arguments.Count);

        for (var i = 0; i < arguments.Count; i++)
        {
            var arg = arguments[i];

            if (arg.Type == typeof(void))
            {
                // Case 0: nested struct already builder-populated via VisitNew
                // the arg IS the block that populates the sub-builder — include it
                body.Add(arg);
            }
            else if (arg.Type.ImplementsInterface(typeof(IArrowArray)))
            {
                // Case 1: alias existing array directly (zero-copy)
                body.Add(
                    Expression.Call(
                        sbExpr,
                        typeof(StructArrayBuilder).GetMethod(
                            nameof(StructArrayBuilder.SetFieldArray))!,
                        Expression.Constant(i),
                        arg));
            }
            else if (arg.Type.Name.Contains("ReadOnlySpan"))
            {
                // Case 2: append span to field builder
                var elemType = arg.Type.GetGenericArguments()[0];
                var arrayType = GetBufferType(elemType);
                var fbExpr = Expression.Call(
                    sbExpr,
                    typeof(StructArrayBuilder).GetMethod(
                            nameof(StructArrayBuilder.GetFieldBuilder))!
                        .MakeGenericMethod(GetConcreteBuilderType(arrayType)),
                    Expression.Constant(i));
                var appendMethod = typeof(ArrowCompute)
                    .GetMethod(nameof(ArrowCompute.AppendToBuilder))!
                    .MakeGenericMethod(elemType);
                body.Add(
                    Expression.Call(
                        null,
                        appendMethod,
                        Expression.Convert(fbExpr, typeof(IArrowArrayBuilder)),
                        arg));
            }
            else
            {
                throw new NotImplementedException(
                    $"Unsupported field type in builder: {arg.Type.Name}");
            }
        }

        return Expression.Block(
            body.Count > 0 ? body : [Expression.Empty()]);
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

        if (param.Type.Name.Contains("ListArray")) return param;

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
            var operandUnderlyingType = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
            var resultUnderlyingType = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
            if (operandUnderlyingType == resultUnderlyingType) return operand;
            var convertMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.ConvertLogical))!
                .MakeGenericMethod(operandUnderlyingType, resultUnderlyingType);
            return Expression.Call(
                null,
                convertMethod,
                [_ctxParam, AccessValues(operand)]);
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

    private static Expression AccessValues(Expression buffer)
    {
        if (buffer.Type.Name.Contains("Span")) return buffer;
        return buffer.Property("Values");
    }

    private static Expression MakeBuffer(Expression spanOrArray)
    {
        if (spanOrArray.Type.ImplementsInterface(typeof(IArrowArray))) return spanOrArray;

        // ReadOnlySpan<byte> is a bitmap — convert to BooleanArray
        if (spanOrArray.Type.IsGenericType
            && spanOrArray.Type.GetGenericArguments()[0] == typeof(byte))
        {
            var method = typeof(ArrowExtensions).GetMethod(nameof(ArrowExtensions.BooleanArrayFromBitmap))!;
            return Expression.Call(
                null,
                method,
                spanOrArray,
                Expression.Property(spanOrArray, "Length"));
        }

        var elementType = PrimitiveBufferElementType(spanOrArray);
        var spanMethod = typeof(ArrowExtensions).GetMethod(nameof(ArrowExtensions.ArrayFromSpan))!
            .MakeGenericMethod(elementType);

        return Expression.Call(null, spanMethod, spanOrArray);
    }

    private Type GetBufferType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        if (underlying == typeof(int)) return typeof(PrimitiveArray<int>);
        if (underlying == typeof(double)) return typeof(PrimitiveArray<double>);

        if (type.ImplementsInterface(typeof(IReadOnlyDictionary<,>)))
        {
            throw new NotImplementedException("not yet");
        }

        if (type.ImplementsInterface(typeof(IEnumerable<>)))
        {
            var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (elementType != null)
            {
                return typeof(ListArray);
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

    private static Type PrimitiveBufferElementType(Expression expression)
    {
        if (expression.Type.ImplementsInterface(typeof(IArrowArray)))
        {
            if (expression.Type == typeof(BooleanArray))
                return typeof(bool);

            return expression.Type.GetGenericArguments()[0];
        }

        if (expression.Type.Name.Contains("Span"))
            // TODO bitmap is problematic
            return expression.Type.GetGenericArguments()[0];

        throw new InvalidOperationException("invalid");
    }
}