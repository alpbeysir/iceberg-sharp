using System.Collections.Immutable;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Iceberg.Net.Query.Expressions;

public class StructArrayBuilder : IArrowArrayBuilder<StructArray, StructArrayBuilder>
{
    private readonly StructType _type;
    private readonly ImmutableArray<IArrowArrayBuilder<IArrowArray>> _builders;

    public StructArrayBuilder(StructType type)
    {
        _type = type;
        _builders =
        [
            ..type.Fields
                .Select(f => CreateFieldBuilder(f.DataType))
        ];
    }

    public ImmutableArray<IArrowArrayBuilder<IArrowArray>> FieldBuilders => _builders;

    public int FieldCount => _builders.Length;

    public T GetFieldBuilder<T>(int index) where T : class, IArrowArrayBuilder
    {
        return (T)_builders[index];
    }

    public StructArray Build(MemoryAllocator allocator = default)
    {
        var arrays = new IArrowArray[_builders.Length];
        for (var i = 0; i < _builders.Length; i++)
            arrays[i] = _builders[i].Build(allocator);

        return new StructArray(_type, Length, arrays, ArrowBuffer.Empty);
    }

    public int Length
    {
        get
        {
            if (_builders.Length == 0) return 0;
            var lengths = _builders.Select(b => b.Length).Distinct().ToList();
            if (lengths.Count != 1)
                throw new InvalidOperationException("Field builders have mismatched lengths");
            return lengths[0];
        }
    }

    public StructArrayBuilder Reserve(int capacity)
    {
        foreach (var b in _builders)
            ((dynamic)b).Reserve(capacity);
        return this;
    }

    public StructArrayBuilder Resize(int length)
    {
        foreach (var b in _builders)
            ((dynamic)b).Resize(length);
        return this;
    }

    public StructArrayBuilder Clear()
    {
        foreach (var b in _builders)
            ((dynamic)b).Clear();
        return this;
    }

    public StructArrayBuilder AppendNull()
    {
        foreach (var b in _builders)
            ((dynamic)b).AppendNull();
        return this;
    }

    private static IArrowArrayBuilder<IArrowArray> CreateFieldBuilder(IArrowType dataType)
    {
        return dataType switch
        {
            Int32Type => new Int32Array.Builder(),
            DoubleType => new DoubleArray.Builder(),
            BooleanType => new BooleanArray.Builder(),
            ListType l => new ListArray.Builder(l),
            StructType s => new StructArrayBuilder(s),
            _ => throw new NotSupportedException($"No field builder for {dataType.TypeId}")
        };
    }
}
