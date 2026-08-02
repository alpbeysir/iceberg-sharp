// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections;
using System.Data.SqlTypes;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Serialization;

/// <summary>
///     Reflection-based serializer for converting arbitrary .NET objects (including anonymous types)
///     to Arrow RecordBatches. Analogous to System.Text.Json's reflection-based path —
///     works without attributes or source generation but is not AOT-safe.
/// </summary>
public static class RecordBatchBuilder
{
    private static readonly NullabilityInfoContext NullabilityInfoContext = new();

    /// <summary>
    ///     Convert a collection of objects to a RecordBatch. Schema is inferred from the
    ///     public readable properties of <typeparamref name="T" />.
    ///     Works with anonymous types, records, classes, and structs.
    /// </summary>
    [RequiresUnreferencedCode(
        "Uses reflection to inspect properties. Use [ArrowSerializable] for AOT-safe serialization.")]
    public static RecordBatch FromObjects<T>(IEnumerable<T> items)
    {
        var list = items as IReadOnlyList<T> ?? items.ToList();

        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead)
            .ToArray();

        var fields = new List<Field>();
        var builders = new List<IColumnBuilder>();

        foreach (PropertyInfo prop in properties)
        {
            Type propType = prop.PropertyType;
            NullabilityInfo nullabilityInfo = NullabilityInfoContext.Create(prop);
            bool refNullable = nullabilityInfo.WriteState == NullabilityState.Nullable;
            DecimalWithAttribute? decimalWith = prop.GetCustomAttribute<DecimalWithAttribute>();
            if (decimalWith is not null && Nullable.GetUnderlyingType(propType) != typeof(SqlDecimal)
                                       && propType != typeof(SqlDecimal))
                throw new InvalidOperationException(
                    $"[DecimalWith] can only be used on SqlDecimal members, but {prop.Name} is {propType.Name}.");

            IArrowType arrowType = decimalWith is null
                ? InferArrowType(propType)
                : new Decimal128Type(decimalWith.Precision, decimalWith.Scale);
            fields.Add(new Field(prop.Name, arrowType, refNullable));
            builders.Add(CreateColumnBuilder(propType, arrowType));
        }

        Schema.Builder schema = new Schema.Builder();
        foreach (Field f in fields) schema.Field(f);

        // Populate builders
        for (int row = 0; row < list.Count; row++)
        {
            T item = list[row]!;
            for (int col = 0; col < properties.Length; col++)
            {
                object? value = properties[col].GetValue(item);
                builders[col].Append(value);
            }
        }

        var arrays = builders.Select(b => b.Build()).ToArray();
        return new RecordBatch(schema.Build(), arrays, list.Count);
    }

    /// <summary>
    ///     Convert a single object to a single-row RecordBatch.
    /// </summary>
    [RequiresUnreferencedCode(
        "Uses reflection to inspect properties. Use [ArrowSerializable] for AOT-safe serialization.")]
    public static RecordBatch FromObject<T>(T item)
    {
        return FromObjects([item]);
    }

    public static bool ImplementsInterface(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        this Type type,
        Type iface)
    {
        return type.GetInterfaces().Any(x => x.IsAssignableTo(iface) || (
            x.IsGenericType &&
            x.GetGenericTypeDefinition() == iface));
    }

    private static IArrowType InferArrowType(Type clrType)
    {
        Type? underlying = Nullable.GetUnderlyingType(clrType);
        if (underlying is not null) return InferArrowType(underlying);

        // TODO fix this nullability garbage
        if (clrType == typeof(string)) return StringType.Default;
        if (clrType == typeof(bool)) return BooleanType.Default;
        if (clrType == typeof(sbyte)) return Int8Type.Default;
        if (clrType == typeof(byte)) return UInt8Type.Default;
        if (clrType == typeof(short)) return Int16Type.Default;
        if (clrType == typeof(ushort)) return UInt16Type.Default;
        if (clrType == typeof(int)) return Int32Type.Default;
        if (clrType == typeof(uint)) return UInt32Type.Default;
        if (clrType == typeof(long)) return Int64Type.Default;
        if (clrType == typeof(ulong)) return UInt64Type.Default;
        if (clrType == typeof(Half)) return HalfFloatType.Default;
        if (clrType == typeof(float)) return FloatType.Default;
        if (clrType == typeof(double)) return DoubleType.Default;
        if (clrType == typeof(decimal))
            throw new NotSupportedException("CLR decimal is not supported. Use SqlDecimal with [DecimalWith(precision, scale)].");
        if (clrType == typeof(SqlDecimal))
            throw new InvalidOperationException(
                "SqlDecimal requires [DecimalWith(precision, scale)] when its Arrow schema is inferred.");
        if (clrType == typeof(DateTime)) return new TimestampType(TimeUnit.Microsecond, "UTC");
        if (clrType == typeof(DateTimeOffset)) return new TimestampType(TimeUnit.Microsecond, "UTC");
        if (clrType == typeof(DateOnly)) return Date32Type.Default;
        if (clrType == typeof(TimeOnly)) return new Time64Type(TimeUnit.Microsecond);
        if (clrType == typeof(TimeSpan)) return DurationType.Microsecond;
        // if (clrType == typeof(Guid)) return (new GuidType(), false);
        if (clrType == typeof(byte[])) return BinaryType.Default;
        if (clrType == typeof(ReadOnlyMemory<byte>)) return BinaryType.Default;

        if (clrType.IsEnum)
            return new DictionaryType(Int16Type.Default, StringType.Default, false);

        // T[] arrays (not byte[] which is handled above)
        if (clrType.IsArray)
        {
            Type elemType = clrType.GetElementType()!;
            IArrowType elemArrow = InferArrowType(elemType);
            return new ListType(new Field("element", elemArrow, false));
        }

        if (clrType.IsGenericType)
        {
            Type genDef = clrType.GetGenericTypeDefinition();
            if (genDef.ImplementsInterface(typeof(IList<>)) || genDef.ImplementsInterface(typeof(ISet<>)))
            {
                Type elemType = clrType.GetGenericArguments()[0];
                IArrowType elemArrow = InferArrowType(elemType);
                return new ListType(new Field("element", elemArrow, false));
            }

            if (genDef.ImplementsInterface(typeof(IDictionary<,>)))
            {
                var args = clrType.GetGenericArguments();
                IArrowType keyArrow = InferArrowType(args[0]);
                IArrowType valArrow = InferArrowType(args[1]);
                return new MapType(new Field("key", keyArrow, false), new Field("value", valArrow, false));
            }
        }

        // Check for [ArrowSerializable] types with source-generated IArrowSerializer<T>
        Schema? genSchema = GetGeneratedArrowSchema(clrType);
        if (genSchema is not null)
        {
            var structFields = new List<Field>(genSchema.FieldsList);
            return new StructType(structFields);
        }

        // Nested object type (anonymous, record, class, struct with readable properties)
        if (clrType.IsClass || clrType.IsValueType)
        {
            var nestedProps = clrType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead)
                .ToArray();
            if (nestedProps.Length > 0)
            {
                var nestedFields = nestedProps.Select(p =>
                {
                    NullabilityInfo nullabilityInfo = NullabilityInfoContext.Create(p);
                    bool refNullable = nullabilityInfo.WriteState == NullabilityState.Nullable;
                    IArrowType ft = InferArrowType(p.PropertyType);
                    return new Field(p.Name, ft, refNullable);
                }).ToList();
                return new StructType(nestedFields);
            }
        }

        throw new NotSupportedException($"Cannot infer Arrow type for {clrType.FullName}");
    }

    /// <summary>
    ///     Check if a type implements IArrowSerializer&lt;T&gt; (i.e. has [ArrowSerializable] source-generated code)
    ///     and return its static ArrowSchema if so.
    /// </summary>
    private static Schema? GetGeneratedArrowSchema(Type clrType)
    {
        Type? iface = clrType.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IArrowSerializer<>));
        if (iface is null) return null;

        PropertyInfo? schemaProp = clrType.GetProperty(
            "ArrowSchema",
            BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        return schemaProp?.GetValue(null) as Schema;
    }

    /// <summary>
    ///     Try to get the static ToRecordBatch(IReadOnlyList&lt;T&gt;) method from a source-generated type.
    /// </summary>
    private static MethodInfo? GetGeneratedToRecordBatchList(Type clrType)
    {
        Type listType = typeof(IReadOnlyList<>).MakeGenericType(clrType);
        return clrType.GetMethod(
            "ToRecordBatch",
            BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy,
            [listType]);
    }

    private static IColumnBuilder CreateColumnBuilder(Type clrType, IArrowType arrowType)
    {
        Type? underlying = Nullable.GetUnderlyingType(clrType);
        if (underlying is not null)
            return CreateColumnBuilder(underlying, arrowType); // inner builders all handle null

        if (clrType == typeof(string)) return new StringColumnBuilder();
        if (clrType == typeof(bool)) return new BoolColumnBuilder();
        if (clrType == typeof(sbyte))
            return new TypedColumnBuilder<sbyte, Int8Array.Builder>(
                new Int8Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(byte))
            return new TypedColumnBuilder<byte, UInt8Array.Builder>(
                new UInt8Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(short))
            return new TypedColumnBuilder<short, Int16Array.Builder>(
                new Int16Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(ushort))
            return new TypedColumnBuilder<ushort, UInt16Array.Builder>(
                new UInt16Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(int))
            return new TypedColumnBuilder<int, Int32Array.Builder>(
                new Int32Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(uint))
            return new TypedColumnBuilder<uint, UInt32Array.Builder>(
                new UInt32Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(long))
            return new TypedColumnBuilder<long, Int64Array.Builder>(
                new Int64Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(ulong))
            return new TypedColumnBuilder<ulong, UInt64Array.Builder>(
                new UInt64Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(Half))
            return new TypedColumnBuilder<Half, HalfFloatArray.Builder>(
                new HalfFloatArray.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(float))
            return new TypedColumnBuilder<float, FloatArray.Builder>(
                new FloatArray.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(double))
            return new TypedColumnBuilder<double, DoubleArray.Builder>(
                new DoubleArray.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(SqlDecimal))
            return new DecimalColumnBuilder((Decimal128Type)arrowType);
        if (clrType == typeof(DateTime)) return new DateTimeColumnBuilder();
        if (clrType == typeof(DateTimeOffset)) return new DateTimeOffsetColumnBuilder();
        if (clrType == typeof(DateOnly))
            return new TypedColumnBuilder<DateOnly, Date32Array.Builder>(
                new Date32Array.Builder(),
                (b, v) => b.Append(v),
                b => b.AppendNull(),
                b => b.Build());
        if (clrType == typeof(TimeOnly)) return new TimeOnlyColumnBuilder();
        if (clrType == typeof(TimeSpan)) return new TimeSpanColumnBuilder();
        // if (clrType == typeof(Guid)) return new GuidColumnBuilder();
        if (clrType == typeof(byte[])) return new BinaryColumnBuilder();
        if (clrType == typeof(ReadOnlyMemory<byte>)) return new ReadOnlyMemoryByteColumnBuilder();
        if (clrType.IsEnum) return new EnumColumnBuilder();

        // List<T>, T[], HashSet<T> → ListArray
        if (arrowType is ListType listType)
        {
            Type elemClrType = clrType.IsArray
                ? clrType.GetElementType()!
                : clrType.GetGenericArguments()[0];
            IColumnBuilder elemBuilder = CreateColumnBuilder(elemClrType, listType.ValueDataType);
            return new ListColumnBuilder(listType, elemClrType, elemBuilder);
        }

        // Dictionary<K,V> → MapArray
        if (arrowType is MapType mapType)
        {
            var args = clrType.GetGenericArguments();
            IColumnBuilder keyBuilder = CreateColumnBuilder(args[0], mapType.KeyField.DataType);
            IColumnBuilder valBuilder = CreateColumnBuilder(args[1], mapType.ValueField.DataType);
            return new MapColumnBuilder(mapType, args[0], args[1], keyBuilder, valBuilder);
        }

        // Nested object → StructArray
        if (arrowType is StructType structType)
        {
            // If the type has source-generated IArrowSerializer<T>, delegate to it
            MethodInfo? toRecordBatchList = GetGeneratedToRecordBatchList(clrType);
            if (toRecordBatchList is not null)
                return new SourceGenStructColumnBuilder(clrType, structType, toRecordBatchList);

            // Otherwise, fall back to reflection-based struct builder
            var nestedProps = clrType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead)
                .ToArray();
            var childBuilders = new List<IColumnBuilder>();
            for (int i = 0; i < nestedProps.Length; i++)
            {
                IArrowType childArrowType = structType.Fields[i].DataType;
                childBuilders.Add(CreateColumnBuilder(nestedProps[i].PropertyType, childArrowType));
            }

            return new StructColumnBuilder(structType, nestedProps, childBuilders);
        }

        throw new NotSupportedException($"Column builder not available for {clrType.FullName}");
    }

    // --- Column builder interface and implementations ---

    private interface IColumnBuilder
    {
        void Append(object? value);
        IArrowArray Build();
    }

    private sealed class StringColumnBuilder : IColumnBuilder
    {
        private readonly StringArray.Builder _b = new();

        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append((string)value);
        }

        public IArrowArray Build()
        {
            return _b.Build();
        }
    }

    private sealed class BoolColumnBuilder : IColumnBuilder
    {
        private readonly BooleanArray.Builder _b = new();

        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append((bool)value);
        }

        public IArrowArray Build()
        {
            return _b.Build();
        }
    }

    private sealed class TypedColumnBuilder<T, TBuilder> : IColumnBuilder
        where T : struct
        where TBuilder : class
    {
        private readonly Action<TBuilder, T> _append;
        private readonly Action<TBuilder> _appendNull;
        private readonly Func<TBuilder, IArrowArray> _build;
        private readonly TBuilder _builder;

        public TypedColumnBuilder(
            TBuilder builder,
            Action<TBuilder, T> append,
            Action<TBuilder> appendNull,
            Func<TBuilder, IArrowArray> build)
        {
            _builder = builder;
            _append = append;
            _appendNull = appendNull;
            _build = build;
        }

        public void Append(object? value)
        {
            if (value is null) _appendNull(_builder);
            else _append(_builder, (T)value);
        }

        public IArrowArray Build()
        {
            return _build(_builder);
        }
    }

    private sealed class DecimalColumnBuilder : IColumnBuilder
    {
        private readonly Decimal128Type _type;
        private readonly List<object?> _values = [];

        public DecimalColumnBuilder(Decimal128Type type)
        {
            _type = type;
        }

        public void Append(object? value)
        {
            _values.Add(value);
        }

        public IArrowArray Build()
        {
            return ArrowArrayHelper.BuildDecimalArray(_values, _type);
        }
    }

    private sealed class DateTimeColumnBuilder : IColumnBuilder
    {
        private readonly TimestampArray.Builder _b = new(new TimestampType(TimeUnit.Microsecond, "UTC"));

        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append(new DateTimeOffset((DateTime)value, TimeSpan.Zero));
        }

        public IArrowArray Build()
        {
            return _b.Build();
        }
    }

    private sealed class DateTimeOffsetColumnBuilder : IColumnBuilder
    {
        private readonly TimestampArray.Builder _b = new(new TimestampType(TimeUnit.Microsecond, "UTC"));

        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append((DateTimeOffset)value);
        }

        public IArrowArray Build()
        {
            return _b.Build();
        }
    }

    private sealed class TimeOnlyColumnBuilder : IColumnBuilder
    {
        private readonly List<(TimeOnly Value, bool IsNull)> _values = [];

        public void Append(object? value)
        {
            if (value is null) _values.Add((default, true));
            else _values.Add(((TimeOnly)value, false));
        }

        public IArrowArray Build()
        {
            Time64Array.Builder b = new Time64Array.Builder(new Time64Type(TimeUnit.Microsecond));
            foreach ((TimeOnly v, bool isNull) in _values)
                if (isNull) b.AppendNull();
                else b.Append(v);
            return b.Build();
        }
    }

    private sealed class TimeSpanColumnBuilder : IColumnBuilder
    {
        private readonly List<(TimeSpan Value, bool IsNull)> _values = [];

        public void Append(object? value)
        {
            if (value is null) _values.Add((default, true));
            else _values.Add(((TimeSpan)value, false));
        }

        public IArrowArray Build()
        {
            DurationArray.Builder b = new DurationArray.Builder(DurationType.Microsecond);
            foreach ((TimeSpan v, bool isNull) in _values)
                if (isNull) b.AppendNull();
                else b.Append(v);
            return b.Build();
        }
    }

    // private sealed class GuidColumnBuilder : IColumnBuilder
    // {
    //     private readonly GuidArray.Builder _b = new();
    //     public void Append(object? value)
    //     {
    //         if (value is null) _b.AppendNull();
    //         else _b.Append((Guid)value);
    //     }
    //     public IArrowArray Build() => _b.Build();
    // }

    private sealed class BinaryColumnBuilder : IColumnBuilder
    {
        private readonly BinaryArray.Builder _b = new();

        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append((ReadOnlySpan<byte>)(byte[])value);
        }

        public IArrowArray Build()
        {
            return _b.Build();
        }
    }

    private sealed class EnumColumnBuilder : IColumnBuilder
    {
        private readonly Dictionary<string, short> _dict = new();
        private readonly List<string?> _values = [];

        public void Append(object? value)
        {
            if (value is null)
            {
                _values.Add(null);
                return;
            }

            string name = value.ToString()!;
            if (!_dict.ContainsKey(name))
                _dict[name] = (short)_dict.Count;
            _values.Add(name);
        }

        public IArrowArray Build()
        {
            string[] dictNames = _dict.OrderBy(kv => kv.Value).Select(kv => kv.Key).ToArray();
            StringArray.Builder dictBuilder = new StringArray.Builder();
            foreach (string n in dictNames) dictBuilder.Append(n);
            StringArray dictArray = dictBuilder.Build();

            Int16Array.Builder idxBuilder = new Int16Array.Builder();
            foreach (string? v in _values)
                if (v is null) idxBuilder.AppendNull();
                else idxBuilder.Append(_dict[v]);

            return new DictionaryArray(
                new DictionaryType(Int16Type.Default, StringType.Default, false),
                idxBuilder.Build(),
                dictArray);
        }
    }

    private sealed class ReadOnlyMemoryByteColumnBuilder : IColumnBuilder
    {
        private readonly BinaryArray.Builder _b = new();

        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append(((ReadOnlyMemory<byte>)value).Span);
        }

        public IArrowArray Build()
        {
            return _b.Build();
        }
    }

    private sealed class ListColumnBuilder : IColumnBuilder
    {
        private readonly IColumnBuilder _elemBuilder;
        private readonly ListType _listType;
        private readonly List<int> _offsets = [0];
        private readonly List<bool> _validity = [];
        private int _totalElements;

        public ListColumnBuilder(ListType listType, Type elemClrType, IColumnBuilder elemBuilder)
        {
            _listType = listType;
            _elemBuilder = elemBuilder;
        }

        public void Append(object? value)
        {
            if (value is null)
            {
                _validity.Add(false);
                _offsets.Add(_totalElements);
                return;
            }

            _validity.Add(true);
            IEnumerable enumerable = (IEnumerable)value;
            foreach (object item in enumerable)
            {
                _elemBuilder.Append(item);
                _totalElements++;
            }

            _offsets.Add(_totalElements);
        }

        public IArrowArray Build()
        {
            IArrowArray valueArray = _elemBuilder.Build();
            int length = _validity.Count;
            int nullCount = _validity.Count(v => !v);

            ArrowBuffer offsetBuffer = new ArrowBuffer(
                _offsets.SelectMany(BitConverter.GetBytes).ToArray());

            ArrowBuffer nullBitmap;
            if (nullCount == 0)
            {
                nullBitmap = ArrowBuffer.Empty;
            }
            else
            {
                byte[] bitmapBytes = new byte[(length + 7) / 8];
                for (int i = 0; i < length; i++)
                    if (_validity[i])
                        bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                nullBitmap = new ArrowBuffer(bitmapBytes);
            }

            ArrayData data = new ArrayData(
                _listType,
                length,
                nullCount,
                0,
                [nullBitmap, offsetBuffer],
                [valueArray.Data]);
            return new ListArray(data);
        }
    }

    private sealed class MapColumnBuilder : IColumnBuilder
    {
        private readonly IColumnBuilder _keyBuilder;
        private readonly MapType _mapType;
        private readonly List<int> _offsets = [0];
        private readonly IColumnBuilder _valBuilder;
        private readonly List<bool> _validity = [];
        private int _totalEntries;

        public MapColumnBuilder(
            MapType mapType,
            Type keyClrType,
            Type valClrType,
            IColumnBuilder keyBuilder,
            IColumnBuilder valBuilder)
        {
            _mapType = mapType;
            _keyBuilder = keyBuilder;
            _valBuilder = valBuilder;
        }

        public void Append(object? value)
        {
            if (value is null)
            {
                _validity.Add(false);
                _offsets.Add(_totalEntries);
                return;
            }

            _validity.Add(true);
            IDictionary dict = (IDictionary)value;
            foreach (DictionaryEntry entry in dict)
            {
                _keyBuilder.Append(entry.Key);
                _valBuilder.Append(entry.Value);
                _totalEntries++;
            }

            _offsets.Add(_totalEntries);
        }

        public IArrowArray Build()
        {
            IArrowArray keyArray = _keyBuilder.Build();
            IArrowArray valArray = _valBuilder.Build();
            int length = _validity.Count;
            int nullCount = _validity.Count(v => !v);

            ArrowBuffer offsetBuffer = new ArrowBuffer(
                _offsets.SelectMany(BitConverter.GetBytes).ToArray());

            ArrowBuffer nullBitmap;
            if (nullCount == 0)
            {
                nullBitmap = ArrowBuffer.Empty;
            }
            else
            {
                byte[] bitmapBytes = new byte[(length + 7) / 8];
                for (int i = 0; i < length; i++)
                    if (_validity[i])
                        bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                nullBitmap = new ArrowBuffer(bitmapBytes);
            }

            // MapArray's child is a StructArray of (key, value) entries
            StructType entryType = new StructType(new List<Field> { _mapType.KeyField, _mapType.ValueField });
            StructArray entryArray = new StructArray(
                entryType,
                _totalEntries,
                [keyArray, valArray],
                ArrowBuffer.Empty);

            ArrayData data = new ArrayData(
                _mapType,
                length,
                nullCount,
                0,
                [nullBitmap, offsetBuffer],
                [entryArray.Data]);
            return new MapArray(data);
        }
    }

    /// <summary>
    ///     Column builder that delegates to source-generated ToRecordBatch(IReadOnlyList&lt;T&gt;)
    ///     for [ArrowSerializable] types, then wraps the RecordBatch columns into a StructArray.
    /// </summary>
    private sealed class SourceGenStructColumnBuilder : IColumnBuilder
    {
        private readonly Type _clrType;
        private readonly List<object?> _items = [];
        private readonly StructType _structType;
        private readonly MethodInfo _toRecordBatchList;

        public SourceGenStructColumnBuilder(Type clrType, StructType structType, MethodInfo toRecordBatchList)
        {
            _clrType = clrType;
            _structType = structType;
            _toRecordBatchList = toRecordBatchList;
        }

        public void Append(object? value)
        {
            _items.Add(value);
        }

        public IArrowArray Build()
        {
            int length = _items.Count;
            int nullCount = _items.Count(v => v is null);

            // Build a typed list for the source-generated method
            Type listType = typeof(List<>).MakeGenericType(_clrType);
            IList typedList = (IList)Activator.CreateInstance(listType, length)!;

            // For null slots, we need a stand-in value (first non-null item)
            object? standIn = _items.FirstOrDefault(v => v is not null);
            foreach (object? item in _items)
                typedList.Add(item ?? standIn!);

            // Call the generated ToRecordBatch(IReadOnlyList<T>)
            RecordBatch batch = (RecordBatch)_toRecordBatchList.Invoke(null, [typedList])!;

            // Extract columns as child arrays for the StructArray
            var childArrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
                childArrays[i] = batch.Column(i);

            // Build null bitmap
            ArrowBuffer nullBitmap;
            if (nullCount == 0)
            {
                nullBitmap = ArrowBuffer.Empty;
            }
            else
            {
                byte[] bitmapBytes = new byte[(length + 7) / 8];
                for (int i = 0; i < length; i++)
                    if (_items[i] is not null)
                        bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                nullBitmap = new ArrowBuffer(bitmapBytes);
            }

            return new StructArray(_structType, length, childArrays, nullBitmap, nullCount);
        }
    }

    private sealed class StructColumnBuilder : IColumnBuilder
    {
        private readonly List<IColumnBuilder> _childBuilders;
        private readonly PropertyInfo[] _properties;
        private readonly StructType _structType;
        private readonly List<bool> _validity = [];

        public StructColumnBuilder(StructType structType, PropertyInfo[] properties, List<IColumnBuilder> childBuilders)
        {
            _structType = structType;
            _properties = properties;
            _childBuilders = childBuilders;
        }

        public void Append(object? value)
        {
            if (value is null)
            {
                _validity.Add(false);
                // Append nulls/defaults to all children to keep lengths aligned
                for (int i = 0; i < _childBuilders.Count; i++)
                    _childBuilders[i].Append(null);
            }
            else
            {
                _validity.Add(true);
                for (int i = 0; i < _properties.Length; i++)
                    _childBuilders[i].Append(_properties[i].GetValue(value));
            }
        }

        public IArrowArray Build()
        {
            var childArrays = _childBuilders.Select(b => b.Build()).ToArray();
            int length = _validity.Count;
            int nullCount = _validity.Count(v => !v);

            // Build null bitmap
            ArrowBuffer nullBitmap;
            if (nullCount == 0)
            {
                nullBitmap = ArrowBuffer.Empty;
            }
            else
            {
                byte[] bitmapBytes = new byte[(length + 7) / 8];
                for (int i = 0; i < length; i++)
                    if (_validity[i])
                        bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                nullBitmap = new ArrowBuffer(bitmapBytes);
            }

            return new StructArray(_structType, length, childArrays, nullBitmap, nullCount);
        }
    }
}
