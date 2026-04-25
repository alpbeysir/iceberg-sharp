using System.Collections.ObjectModel;
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
    // public required VirtualBuffer Arena { get; init; }
    // public required VirtualArenaManager Manager { get; init; }
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
}

public static class ArrowCompute
{
    private static IArrowType GetArrowType<T>()
    {
        if (typeof(T) == typeof(int)) return new Int32Type();
        if (typeof(T) == typeof(long)) return new Int64Type();
        if (typeof(T) == typeof(double)) return new DoubleType();
        if (typeof(T) == typeof(float)) return new FloatType();
        if (typeof(T) == typeof(bool)) return new BooleanType();

        throw new NotSupportedException($"The type {typeof(T).FullName} is not supported.");
    }

    private static PrimitiveArray<T> FromBuffer<T>(ArrowBuffer buffer, int length)
        where T : struct, INumber<T>
    {
        var result = (PrimitiveArray<T>)ArrowArrayFactory.BuildArray(
            new ArrayData(GetArrowType<T>(), length, 0, 0, [ArrowBuffer.Empty, buffer], []));
        return result;
    }

    // TODO this will not handle nulls properly
    // TODO allocate and return span to allow efficient chaining
    public static PrimitiveArray<T> Zip<T>(
        ExecutionContext ctx,
        ReadOnlySpan<T> l,
        ReadOnlySpan<T> r,
        ExpressionType expressionType)
        where T : struct, INumber<T>
    {
        // TODO precompute these switches
        Func<Vector<T>, Vector<T>, Vector<T>> vectorSelector = expressionType switch
        {
            ExpressionType.Add => Vector.Add,
            ExpressionType.Subtract => Vector.Subtract,
            ExpressionType.Multiply => Vector.Multiply,
            ExpressionType.Divide => Vector.Divide,
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
        Func<T, T, T> selector = expressionType switch
        {
            ExpressionType.Add => (n1, n2) => n1 + n2,
            ExpressionType.Subtract => (n1, n2) => n1 - n2,
            ExpressionType.Multiply => (n1, n2) => n1 * n2,
            ExpressionType.Divide => (n1, n2) => n1 / n2,
            _ => throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null)
        };
        var zip = l.AsVectorizable().Zip(
            r,
            vectorSelector,
            selector);
        var builder = new ArrowBuffer.Builder<T>(l.Length);
        builder.Resize(l.Length);
        zip.CopyTo(builder.Span);
        return FromBuffer<T>(builder.Build(), l.Length);
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

    public static PrimitiveArray<TResult> Convert<T, TResult>(
        ExecutionContext ctx,
        ReadOnlySpan<T> span)
        where T : struct, INumber<T>
        where TResult : struct, INumber<TResult>
    {
        var builder = new ArrowBuffer.Builder<TResult>(span.Length);
        builder.Resize(span.Length);
        var resultSpan = builder.Span;
        for (var i = 0; i < resultSpan.Length; i++)
        {
            resultSpan[i] = TResult.CreateChecked(span[i]);
        }

        return FromBuffer<TResult>(builder.Build(), span.Length);
    }
}

public class BufferTransformVisitor : ExpressionVisitorNarrow<Expression, LambdaExpression, Expression,
    NewExpression, ElementInit, MemberBinding, MemberAssignment, MemberListBinding, MemberMemberBinding>
{
    private readonly Dictionary<string, ParameterExpression> _bindings = [];
    private readonly ParameterExpression _ctxParam = Expression.Parameter(typeof(ExecutionContext), "ctx");
    private readonly Stack<ParameterExpression> _indexVars = [];
    private readonly Dictionary<MemberInfo, int> _memberIndex = [];
    private QueryStepVisitor QueryStepVisitor => new([]);

    protected override Expression MakeBinary(
        BinaryExpression node,
        Expression left,
        LambdaExpression conversion,
        Expression right)
    {
        var zipMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Zip))
            .MakeGenericMethod(node.Type);
        return Expression.Call(
            null,
            zipMethod,
            [_ctxParam, left.Property("Values"), right.Property("Values"), node.NodeType.Quoted]);
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
        return ColumnBuffers.MakeConstantBuffer(node.Type, node.Value, int.MaxValue).Quoted;
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
            var convertMethod = typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Convert))
                .MakeGenericMethod(node.Operand.Type, node.Type);
            return Expression.Call(
                null,
                convertMethod,
                [_ctxParam, operand.Property("Values")]);
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
        return Unsafe.As<T>(arr.Fields[index]);
    }

    private static Span<T> ArenaAllocateHelper<T>(VirtualBuffer buffer, int amount) where T : struct
    {
        return MemoryMarshal.Cast<byte, T>(buffer.AllocateRange(Unsafe.SizeOf<T>() * amount));
    }
}