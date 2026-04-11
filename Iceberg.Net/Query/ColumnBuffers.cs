using System.Buffers;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Reflection;
using Iceberg.Net.Misc;
using ParquetSharp;
using Array = Apache.Arrow.Array;

namespace Iceberg.Net.Query;

public sealed record ColumnBufferSet : IDisposable
{
    public ColumnBufferSet(Dictionary<string, IColumnBuffer> buffers)
    {
        Buffers = buffers.ToImmutableDictionary();
    }

    public ColumnBufferSet(ImmutableDictionary<string, IColumnBuffer> buffers)
    {
        Buffers = buffers;
    }

    public ImmutableDictionary<string, IColumnBuffer> Buffers { get; }

    public int Length => Buffers.Select(buf => buf.Value.Count).First();

    public void Dispose()
    {
        foreach (var buffers in Buffers.Values) buffers.Dispose();
    }

    public bool SanityCheck()
    {
        return Buffers.Select(buf => buf.Value.Count).Distinct().Count() <= 1;
    }
}

public interface IColumnBuffer : IDisposable
{
    public int Count { get; }
    public Type ElementType { get; }

    [Pure]
    public IColumnBuffer Slice(int size);

    [Pure]
    public Expression AccessRead(Expression self, Expression index);

    [Pure]
    public Expression AccessReadWrite(Expression self, Expression index);
}

public interface IPooledBuffer : IColumnBuffer
{
    public object Parameter { get; }

    [Pure]
    public ParameterExpression ParameterExpr();
}

public interface IConstantBuffer : IColumnBuffer;

internal sealed record ConstantColumnBuffer<T>(T? TypedValue, int Count) : IConstantBuffer
{
    public void Dispose()
    {
    }

    public Type ElementType => typeof(T);

    public IColumnBuffer Slice(int size)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(size, Count);
        return this with { Count = size };
    }

    public Expression AccessRead(Expression self, Expression index)
    {
        return Expression.Constant(TypedValue);
    }

    public Expression AccessReadWrite(Expression self, Expression index)
    {
        throw new InvalidOperationException();
    }

    internal static ConstantColumnBuffer<T> Create(T value, int count)
    {
        return new ConstantColumnBuffer<T>(value, count);
    }
}

internal sealed record PooledColumnBuffer<T>(T[] TypedBuffer, int Count) : IPooledBuffer
{
    private bool _disposed;

    public void Dispose()
    {
        if (!Interlocked.CompareExchange(ref _disposed, true, false))
            ArrayPool<T>.Shared.Return(TypedBuffer);
    }

    public Type ElementType => typeof(T);

    public IColumnBuffer Slice(int size)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(size, Count);
        return new PooledColumnBuffer<T>(TypedBuffer, size);
    }

    public ParameterExpression ParameterExpr()
    {
        return Expression.Parameter(typeof(T[]), "pooled");
    }

    public Expression AccessRead(Expression self, Expression index)
    {
        return AccessReadWrite(self, index);
    }

    public Expression AccessReadWrite(Expression self, Expression index)
    {
        return Expression.ArrayAccess(self, index);
    }

    public object Parameter => TypedBuffer;

    internal static PooledColumnBuffer<T> Create(int size)
    {
        var buf = ArrayPool<T>.Shared.Rent(size);
        return new PooledColumnBuffer<T>(buf, size);
    }
}

internal sealed record ArrowColumnBuffer(Array ArrowArray) : IPooledBuffer
{
    public void Dispose()
    {
        ArrowArray.Dispose();
    }

    public int Count => ArrowArray.Length;

    public Type ElementType
    {
        get
        {
            var type = ArrowArray.GetType();
            if (type.ImplementsInterface(typeof(IReadOnlyList<>)))
                return type.GetInterface("IReadOnlyList")!.GetGenericArguments()[0];
            else
                throw new InvalidOperationException("Must be converted before access");
        }
    }

    public IColumnBuffer Slice(int size)
    {
        return this with { ArrowArray = ArrowArray[..size] };
    }

    public ParameterExpression ParameterExpr()
    {
        if (ArrowArray.NullCount == 0)
            // TODO optimize this case
            throw new NotImplementedException();
        else
            return Expression.Parameter(ArrowArray.GetType(), "arrow");
    }

    public Expression AccessRead(Expression self, Expression index)
    {
        return Expression.ArrayAccess(self, index);
    }

    public Expression AccessReadWrite(Expression self, Expression index)
    {
        throw new InvalidOperationException("Arrow buffers are read-only");
    }

    public object Parameter => ArrowArray;
}

internal static class ColumnBuffers
{
    private const string NestedValueAccess = "Value";

    internal static IConstantBuffer MakeConstantBuffer(Type elementType, object? val, int size)
    {
        var method = typeof(ConstantColumnBuffer<>)
            .MakeGenericType(elementType)
            .GetMethod(
                nameof(ConstantColumnBuffer<>.Create),
                BindingFlags.NonPublic | BindingFlags.Static)!;
        return (IConstantBuffer)method.Invoke(null, [val, size])!;
    }

    internal static IPooledBuffer MakePooledBuffer(Type elementType, int size)
    {
        var method = typeof(PooledColumnBuffer<>)
            .MakeGenericType(elementType)
            .GetMethod(
                nameof(PooledColumnBuffer<>.Create),
                BindingFlags.NonPublic | BindingFlags.Static)!;
        return (IPooledBuffer)method.Invoke(null, [size])!;
    }

    internal static Expression WrapToTargetType(Expression value, Type target)
    {
        if (value.Type == target) return value;

        switch (target.IsGenericType)
        {
            case true when target.GetGenericTypeDefinition() == typeof(Nested<>):
            {
                var innerType = target.GetGenericArguments()[0];
                var wrappedInner = WrapToTargetType(value, innerType);
                var ctor = target.GetConstructor([innerType])!;
                return Expression.New(ctor, wrappedInner);
            }
            case true when target.GetGenericTypeDefinition() == typeof(Nullable<>):
            {
                var innerType = target.GetGenericArguments()[0];
                var wrappedInner = WrapToTargetType(value, innerType);
                return Expression.Convert(wrappedInner, target);
            }
            default:
                return Expression.Convert(value, target);
        }
    }

    internal static Expression NullableHasValue(Expression value)
    {
        return Expression.PropertyOrField(value, "HasValue");
    }

    internal static Expression MaybeUnwrapNested(Expression value)
    {
        if (value.Type.IsGenericType && value.Type.GetGenericTypeDefinition() == typeof(Nested<>))
            return Expression.PropertyOrField(value, NestedValueAccess);
        return value;
    }

    internal static Expression UnwrapNullable(Expression value)
    {
        var nullableValue = Expression.PropertyOrField(value, NestedValueAccess);
        if (nullableValue.Type.IsGenericType && nullableValue.Type.GetGenericTypeDefinition() == typeof(Nested<>))
            return Expression.PropertyOrField(nullableValue, NestedValueAccess);
        return nullableValue;
    }
}