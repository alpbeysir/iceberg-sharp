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
    private static PrimitiveArray<T> FromZipVectorizable<T>(ZipVectorizable<T, T> zip, int length)
        where T : struct, INumber<T>
    {
        var builder = new ArrowBuffer.Builder<T>(length);
        zip.CopyTo(builder.Span);
        var result = (PrimitiveArray<T>)ArrowArrayFactory.BuildArray(
            new ArrayData(new Int32Type(), length, 0, 0, [ArrowBuffer.Empty, builder.Build()], []));
        return result;
    }

    public static PrimitiveArray<T> Add<T>(ExecutionContext ctx, ReadOnlySpan<T> l, ReadOnlySpan<T> r)
        where T : struct, INumber<T>
    {
        var zip = l.AsVectorizable().Zip(
            r,
            Vector.Add,
            (number, equatable) => number + equatable);
        return FromZipVectorizable(zip, l.Length);
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
        var implMethod = node.NodeType switch
        {
            ExpressionType.Add => typeof(ArrowCompute).GetMethod(nameof(ArrowCompute.Add))
                .MakeGenericMethod(left.Type.GetGenericArguments()[0])!,
            _ => throw new ArgumentOutOfRangeException()
        };
        return Expression.Call(null, implMethod, [_ctxParam, left.Property("Values"), right.Property("Values")]);
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