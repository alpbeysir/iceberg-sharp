using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Iceberg.Net.Query.Expressions;

public class StructArrayBuilder : IArrowArrayBuilder<StructArray, StructArrayBuilder>
{
    private readonly StructType _type;
    private readonly IArrowArrayBuilder<IArrowArray>?[] _fieldBuilders;
    private readonly IArrowArray?[] _fieldArrays;

    public StructArrayBuilder(StructType type)
    {
        _type = type;
        var count = type.Fields.Count;
        _fieldBuilders = new IArrowArrayBuilder<IArrowArray>?[count];
        _fieldArrays = new IArrowArray?[count];
    }

    public int FieldCount => _fieldArrays.Length;

    /// <summary>Alias an existing array as a field — no copy.</summary>
    public StructArrayBuilder SetFieldArray(int index, IArrowArray array)
    {
        if (_fieldBuilders[index] != null)
            throw new InvalidOperationException(
                $"Field {index} already has a builder; cannot alias.");
        _fieldArrays[index] = array;
        return this;
    }

    public T GetFieldBuilder<T>(int index) where T : class, IArrowArrayBuilder
    {
        _fieldBuilders[index] ??= CreateFieldBuilder(_type.Fields[index].DataType);
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
