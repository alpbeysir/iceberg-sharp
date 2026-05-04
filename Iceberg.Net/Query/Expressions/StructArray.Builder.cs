using System.Collections.Immutable;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Iceberg.Net.Query.Expressions;

public class StructArrayBuilder : IArrowArrayBuilder<StructArray, StructArrayBuilder>
{
    private readonly StructType _type;
    private readonly ImmutableArray<IArrowArrayBuilder> _builders;

    public StructArrayBuilder(StructType type)
    {
        _type = type;
        _builders = [];
    }

    public StructArray Build(MemoryAllocator allocator)
    {
        throw new NotImplementedException();
    }

    public int Length
    {
        get
        {
            var lengths = _builders.Select(b => b.Length).Distinct().ToList();
            if (lengths.Count != 1) throw new InvalidOperationException("impossible");
            return lengths.First();
        }
    }

    public StructArrayBuilder Reserve(int capacity)
    {
        foreach (var b in _builders)
            if (b is MapArray.Builder mb) mb.Reserve(capacity);
            else if (b is ListArray.Builder lb) lb.Reserve(capacity);
        return this;
    }

    public StructArrayBuilder Resize(int length)
    {
        foreach (var b in _builders)
            if (b is MapArray.Builder mb) mb.Resize(length);
            else if (b is ListArray.Builder lb) lb.Resize(length);
        return this;
    }

    public StructArrayBuilder Clear()
    {
        foreach (var b in _builders)
            if (b is MapArray.Builder mb) mb.Clear();
            else if (b is ListArray.Builder lb) lb.Clear();
        return this;
    }

    public StructArrayBuilder AppendNull()
    {
        throw new NotImplementedException();
    }
}