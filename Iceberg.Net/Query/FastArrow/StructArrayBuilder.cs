using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Iceberg.Net.Query.Expressions;

namespace Iceberg.Net.Query.FastArrow;

public class StructArrayBuilder : IArrowArrayBuilder<StructArray, StructArrayBuilder>
{
    public MemoryAllocator? Allocator { get; }
    private readonly StructType _type;
    private readonly IArrowArrayBuilder<IArrowArray>?[] _fieldBuilders;
    private readonly IArrowArray?[] _fieldArrays;

    public StructArrayBuilder(StructType dataType, MemoryAllocator? allocator = null)
    {
        Allocator = allocator;
        _type = dataType;
        var count = dataType.Fields.Count;
        _fieldBuilders = new IArrowArrayBuilder<IArrowArray>?[count];
        _fieldArrays = new IArrowArray?[count];
    }

    public int FieldCount => _fieldArrays.Length;

    /// <summary>Alias an existing array as a field — no copy.</summary>
    public StructArrayBuilder SetFieldArray(int index, IArrowArray array)
    {
        // For now we need to copy to prevent ownership issues - will fix later
        var copy = ArrowArrayFactory.BuildArray(array.Data.Clone(Allocator));
        if (_fieldBuilders[index] != null)
            throw new InvalidOperationException(
                $"Field {index} already has a builder; cannot alias.");
        _fieldArrays[index] = copy;
        return this;
    }

    public T GetFieldBuilder<T>(int index) where T : class, IArrowArrayBuilder
    {
        _fieldBuilders[index] ??= ArrowCompute.MakeBuilderFor(_type.Fields[index].DataType, Allocator);
        return (T)_fieldBuilders[index]!;
    }

    public StructArray Build(MemoryAllocator? allocator = null)
    {
        var arrays = new IArrowArray[FieldCount];
        for (var i = 0; i < FieldCount; i++)
        {
            arrays[i] = _fieldArrays[i]
                        ?? _fieldBuilders[i]?.Build(allocator)
                        ?? throw new InvalidOperationException(
                            $"Field {i} ('{_type.Fields[i].Name}') not initialized.");
        }

        return new StructArray(_type, Length, arrays, ArrowBuffer.Empty);
    }

    public int Length
    {
        get
        {
            // Use the first available source for length
            for (var i = 0; i < FieldCount; i++)
            {
                if (_fieldArrays[i] is { } arr) return arr.Length;
                if (_fieldBuilders[i] is { } b) return b.Length;
            }

            return 0;
        }
    }

    public StructArrayBuilder Reserve(int capacity)
    {
        foreach (var b in _fieldBuilders)
            if (b != null)
                ((dynamic)b).Reserve(capacity);
        return this;
    }

    public StructArrayBuilder Resize(int length)
    {
        foreach (var b in _fieldBuilders)
            if (b != null)
                ((dynamic)b).Resize(length);
        return this;
    }

    public StructArrayBuilder Clear()
    {
        for (var i = 0; i < FieldCount; i++)
        {
            if (_fieldBuilders[i] != null)
                ((dynamic)_fieldBuilders[i]!).Clear();
            _fieldBuilders[i] = null;
            _fieldArrays[i] = null;
        }

        return this;
    }

    public StructArrayBuilder AppendNull()
    {
        foreach (var b in _fieldBuilders)
            if (b != null)
                ((dynamic)b).AppendNull();
        return this;
    }
}
