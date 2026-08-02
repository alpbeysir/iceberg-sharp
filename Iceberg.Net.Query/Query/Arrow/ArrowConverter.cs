using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Iceberg.Net.Query.FastArrow;
using Array = System.Array;

namespace Iceberg.Net.Query.Arrow;

public static class ArrowConverter
{
    // Cache Generic Type method definition to avoid reflection in every cycle
    private static readonly MethodInfo _buildMethodDef = typeof(ArrowConverter)
                                                             .GetMethod(
                                                                 "Build",
                                                                 BindingFlags.Static | BindingFlags.Public |
                                                                 BindingFlags.NonPublic)
                                                         ?? throw new InvalidOperationException(
                                                             "ArrowConverter.Build<T> method not found.");

    /// <summary>
    ///     Build Empty RecordBatch with Schema Only
    /// </summary>
    public static RecordBatch GetEmptyBatch<T>()
    {
        Schema schema = SchemaCache<T>.Default;

        StructArray emptyStruct = StructBuilderHelper.BuildStructArray(Enumerable.Empty<T>());

        return new RecordBatch(schema, emptyStruct.Fields, 0);
    }

    /// <summary>
    ///     Convert any IEnumerable/Array into Arrow Array
    /// </summary>
    public static IArrowArray? BuildSingleColumn(object colValue)
    {
        if (colValue == null) return null;

        // Get Element type
        Type? elemType = ArrowTypeResolver.GetEnumerableElementType(colValue.GetType());

        if (elemType == null)
            return null;

        // Build Generic Method
        MethodInfo buildMethod = _buildMethodDef.MakeGenericMethod(elemType);

        try
        {
            // Call Apache.Arrow Build Method
            return (IArrowArray)buildMethod.Invoke(null, [colValue])!;
        }
        catch (TargetInvocationException ex)
        {
            throw ex.InnerException ?? ex;
        }
    }

    /// <summary>
    ///     General Entry：Decide which type of Arrary based on the type of T
    /// </summary>
    public static IArrowArray Build<T>(IEnumerable<T> data)
    {
        Type type = typeof(T);

        // =====================================================================
        // 1. Array Interception (Must comes First!)
        // =====================================================================
        if (type.IsArray)
        {
            Type elemType = type.GetElementType()!;
            MethodInfo method = typeof(ArrowConverter)
                .GetMethod(nameof(BuildListArray), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(elemType);
            return (IArrowArray)method.Invoke(null, [data])!;
        }

        // =====================================================================
        // 2. List Interception
        // =====================================================================
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            Type elemType = type.GetGenericArguments()[0];
            MethodInfo method = typeof(ArrowConverter)
                .GetMethod(nameof(BuildListArray), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(elemType);
            return (IArrowArray)method.Invoke(null, [data])!;
        }

        Type? underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        Type checkType = underlyingType ?? type;

        // 4. Primitives & String
        if (checkType == typeof(int)) return BuildInt32(data.Cast<int?>());
        if (checkType == typeof(uint)) return BuildUInt32(data.Cast<uint?>());
        if (checkType == typeof(string)) return BuildString(data.Cast<string?>());
        if (checkType == typeof(double)) return BuildDouble(data.Cast<double?>());
        if (checkType == typeof(bool)) return BuildBoolean(data.Cast<bool?>());
        if (checkType == typeof(byte)) return BuildUInt8(data.Cast<byte?>());
        if (checkType == typeof(sbyte)) return BuildInt8(data.Cast<sbyte?>());
        if (checkType == typeof(long)) return BuildInt64(data.Cast<long?>());
        if (checkType == typeof(ulong)) return BuildUInt64(data.Cast<ulong?>());
        if (checkType == typeof(short)) return BuildInt16(data.Cast<short?>());
        if (checkType == typeof(ushort)) return BuildUInt16(data.Cast<ushort?>());
        if (checkType == typeof(Half)) return BuildFloat16(data.Cast<Half?>());
        if (checkType == typeof(float)) return BuildFloat32(data.Cast<float?>());
        if (checkType == typeof(decimal)) return BuildDecimal(data.Cast<decimal?>());
        if (checkType == typeof(DateOnly)) return BuildDate32(data.Cast<DateOnly?>());
        if (checkType == typeof(TimeOnly)) return BuildTime64(data.Cast<TimeOnly?>());
        if (checkType == typeof(DateTime)) return BuildTimestamp(data.Cast<DateTime?>());
        if (checkType == typeof(DateTimeOffset)) return BuildDateTimeOffset(data.Cast<DateTimeOffset?>());
        if (checkType == typeof(TimeSpan)) return BuildDuration(data.Cast<TimeSpan?>());
        if (checkType == typeof(Guid)) return BuildGuid(data.Cast<Guid?>());
        if (checkType == typeof(byte[])) return BuildBinary(data.Cast<byte[]?>());
        if (checkType == typeof(Half)) return BuildFloat16(data.Cast<Half?>());
        Type? elementType = ArrowTypeResolver.GetEnumerableElementType(type);
        if (elementType != null)
        {
            MethodInfo method = typeof(ArrowConverter)
                .GetMethod(nameof(BuildListArray), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(elementType);

            return (IArrowArray)method.Invoke(null, [data])!;
        }

        // 5. Struct / Class / Object
        if (type.IsClass || type.IsValueType)
        {
            // [SMART RECOVERY] Handle Object[] that actually contains Structs
            if (checkType == typeof(object))
            {
                IList<T> dataList = data as IList<T> ?? data.ToList();
                T? firstItem = dataList.FirstOrDefault(x => x != null);

                if (firstItem != null)
                {
                    Type runtimeType = firstItem.GetType();
                    // If runtime type is complex (Anonymous/Class) and NOT string
                    if (runtimeType.IsClass && runtimeType != typeof(string))
                        try
                        {
                            // Try to recover via StructBuilder
                            MethodInfo method = typeof(StructBuilderHelper)
                                .GetMethod(
                                    nameof(StructBuilderHelper.BuildStructArray),
                                    BindingFlags.Public | BindingFlags.Static)!
                                .MakeGenericMethod(runtimeType);

                            // We need to Cast<RuntimeType>
                            MethodInfo castMethod = typeof(Enumerable).GetMethod(
                                    nameof(Enumerable.Cast),
                                    BindingFlags.Public | BindingFlags.Static)!
                                .MakeGenericMethod(runtimeType);
                            object? castData = castMethod.Invoke(null, [dataList]);

                            return (IArrowArray)method.Invoke(null, [castData!])!;
                        }
                        catch
                        {
                            // Fallback to String if recovery fails
                        }
                }

                // Default Object fallback: ToString
                StringViewArray.Builder stringBuilder = new();
                foreach (T item in data)
                    if (item == null) stringBuilder.AppendNull();
                    else stringBuilder.Append(item.ToString());
                return stringBuilder.Build();
            }

            // Normal Struct Path
            try
            {
                return StructBuilderHelper.BuildStructArray(data);
            }
            catch (Exception ex)
            {
                throw new NotSupportedException(
                    $"Type {type.FullName} (Underlying: {checkType.FullName}) is not supported yet.",
                    ex);
            }
        }

        // 6. Fallback via Resolver (Recursive catch-all)

        throw new NotSupportedException(
            $"Type {type.FullName} (Underlying: {checkType.FullName}) is not supported yet.");
    }

    /// <summary>
    ///     Build ListArray
    /// </summary>
    public static ListArray BuildListArray<U>(IEnumerable<IEnumerable<U>?> data)
    {
        // Recursion Logic: Flatten -> Build<U>
        // This handles List<Struct>, List<List<int>>, etc.
        List<U> flattenedData = new();

        Int32Array.Builder offsetsBuilder = new();
        BooleanArrayBuilder validityBuilder = new();

        int currentOffset = 0;
        offsetsBuilder.Append(0);

        int nullCount = 0;

        foreach (IEnumerable<U>? subList in data)
            if (subList == null)
            {
                validityBuilder.Append(false);
                offsetsBuilder.Append(currentOffset);
                nullCount++;
            }
            else
            {
                validityBuilder.Append(true);

                int count = 0;
                foreach (U item in subList)
                {
                    flattenedData.Add(item);
                    count++;
                }

                currentOffset += count;
                offsetsBuilder.Append(currentOffset);
            }

        // Recursive Call!
        IArrowArray valuesArray = Build(flattenedData);

        Int32Array? offsetsArray = offsetsBuilder.Build();
        BooleanArray validityArray = validityBuilder.Build();

        ListType listType = new(valuesArray.Data.DataType);

        return new ListArray(
            listType,
            data.Count(),
            offsetsArray.ValueBuffer,
            valuesArray,
            validityArray.ValueBuffer,
            nullCount
        );
    }

    private static HalfFloatArray BuildFloat16(IEnumerable<Half?> data)
    {
        HalfFloatArray.Builder b = new();
        foreach (Half? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static FloatArray BuildFloat32(IEnumerable<float?> data)
    {
        FloatArray.Builder b = new();
        foreach (float? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static Decimal128Array BuildDecimal(IEnumerable<decimal?> data)
    {
        Decimal128Type type = new(38, 18);
        Decimal128Array.Builder b = new(type);
        foreach (decimal? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static Int8Array BuildInt8(IEnumerable<sbyte?> data)
    {
        Int8Array.Builder b = new();
        foreach (sbyte? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static UInt8Array BuildUInt8(IEnumerable<byte?> data)
    {
        UInt8Array.Builder b = new();
        foreach (byte? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static Int16Array BuildInt16(IEnumerable<short?> data)
    {
        Int16Array.Builder b = new();
        foreach (short? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static UInt16Array BuildUInt16(IEnumerable<ushort?> data)
    {
        UInt16Array.Builder b = new();
        foreach (ushort? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static Int32Array BuildInt32(IEnumerable<int?> data)
    {
        Int32Array.Builder b = new();
        foreach (int? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static Int64Array BuildInt64(IEnumerable<long?> data)
    {
        Int64Array.Builder b = new();
        foreach (long? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static UInt32Array BuildUInt32(IEnumerable<uint?> data)
    {
        UInt32Array.Builder b = new();
        foreach (uint? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static UInt64Array BuildUInt64(IEnumerable<ulong?> data)
    {
        UInt64Array.Builder b = new();
        foreach (ulong? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static DoubleArray BuildDouble(IEnumerable<double?> data)
    {
        DoubleArray.Builder b = new();
        foreach (double? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static BooleanArray BuildBoolean(IEnumerable<bool?> data)
    {
        BooleanArray.Builder b = new();
        foreach (bool? v in data)
            if (v.HasValue) b.Append(v.Value);
            else b.AppendNull();
        return b.Build();
    }

    private static StringViewArray BuildString(IEnumerable<string?> data)
    {
        StringViewArray.Builder b = new();
        foreach (string? v in data) b.Append(v);
        return b.Build();
    }

    // DateOnly -> Date32 (Days since epoch)
    private static Date32Array BuildDate32(IEnumerable<DateOnly?> data)
    {
        Date32Array.Builder b = new();
        foreach (DateOnly? v in data)
            if (v.HasValue) b.Append(v.Value.ToDateTime(TimeOnly.MinValue));
            else b.AppendNull();
        return b.Build();
    }

    // TimeOnly -> Time64 (Nanoseconds)
    private static Time64Array BuildTime64(IEnumerable<TimeOnly?> data)
    {
        Time64Array.Builder b = new(TimeUnit.Nanosecond);

        foreach (TimeOnly? v in data)
            if (v.HasValue)
                b.Append(v.Value.Ticks * 100L);
            else
                b.AppendNull();

        return b.Build();
    }

    // DateTime -> Timestamp (Microsecond)
    private static TimestampArray BuildTimestamp(IEnumerable<DateTime?> data)
    {
        TimestampArray.Builder b = new(TimeUnit.Microsecond);

        foreach (DateTime? v in data)
            if (v.HasValue)
            {
                DateTime dt = v.Value;

                DateTimeOffset dto = new(dt.Ticks, TimeSpan.Zero);

                // Ticks (100ns) -> Microsecond (1000ns)
                b.Append(dto);
            }
            else
            {
                b.AppendNull();
            }

        return b.Build();
    }

    private static TimestampArray BuildDateTimeOffset(IEnumerable<DateTimeOffset?> data)
    {
        TimestampArray.Builder b = new(TimeUnit.Microsecond);

        foreach (DateTimeOffset? v in data)
            if (v.HasValue)
                b.Append(v.Value);
            else
                b.AppendNull();

        return b.Build();
    }

    private static DurationArray BuildDuration(IEnumerable<TimeSpan?> data)
    {
        DurationArray.Builder b = new(DurationType.Microsecond);

        foreach (TimeSpan? v in data)
            if (v.HasValue)
                // Ticks (100ns) -> Microseconds (1000ns)
                b.Append(v.Value.Ticks / 10L);
            else
                b.AppendNull();

        return b.Build();
    }

    private static BinaryViewArray BuildGuid(IEnumerable<Guid?> data)
    {
        BinaryViewArray.Builder b = new();
        Span<byte> guidBytes = stackalloc byte[16];
        foreach (Guid? v in data)
            if (v.HasValue)
            {
                v.Value.TryWriteBytes(guidBytes);
                b.Append(guidBytes);
            }
            else
            {
                b.AppendNull();
            }

        return b.Build();
    }

    private static BinaryViewArray BuildBinary(IEnumerable<byte[]?> data)
    {
        BinaryViewArray.Builder b = new();
        foreach (byte[]? v in data)
            if (v != null) b.Append(v);
            else b.AppendNull();
        return b.Build();
    }

    public static Schema GetSchemaFromType<T>()
    {
        Type type = typeof(T);
        PropertyInfo[] members = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        List<Field> fields = new();

        foreach (PropertyInfo member in members)
        {
            Type memberType = member.PropertyType;
            object? dummyInstance = CreateDummyInstance(memberType);

            Array wrapper = Array.CreateInstance(memberType, 1);
            if (dummyInstance != null) wrapper.SetValue(dummyInstance, 0);

            IArrowArray? dummyArr = BuildSingleColumn(wrapper);
            if (dummyArr == null) continue;

            Field field = new(member.Name, dummyArr.Data.DataType, true);
            fields.Add(field);
        }

        return new Schema(fields, null);
    }

    private static object? CreateDummyInstance(Type t)
    {
        if (t == typeof(string)) return string.Empty;
        if (t.IsArray) return Array.CreateInstance(t.GetElementType()!, 0);

        // C# List
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(List<>)) return Activator.CreateInstance(t);

        if (t.IsValueType) return Activator.CreateInstance(t);

        try
        {
            return Activator.CreateInstance(t);
        }
        catch
        {
            try
            {
                return RuntimeHelpers.GetUninitializedObject(t);
            }
            catch
            {
                return null;
            }
        }
    }

    private static object? GetDefault(Type t)
    {
        return t.IsValueType ? Activator.CreateInstance(t) : null;
    }

    /// <summary>
    ///     Slice IEnumerable<RecordBatch> to chuncks and convert it to ArrowBatchs
    /// </summary>
    public static IEnumerable<RecordBatch> ToArrowBatches<T>(IEnumerable<T> data, int batchSize)
    {
        StructArray dummyStruct = StructBuilderHelper.BuildStructArray(Enumerable.Empty<T>());

        StructType? structType = (StructType)dummyStruct.Data.DataType;
        Schema schema = new(structType.Fields, null);

        bool hasYielded = false;

        foreach (T[] chunk in data.Chunk(batchSize))
        {
            hasYielded = true;
            StructArray structArray = StructBuilderHelper.BuildStructArray(chunk);

            yield return new RecordBatch(schema, structArray.Fields, chunk.Length);
        }

        if (!hasYielded) yield return new RecordBatch(schema, dummyStruct.Fields, 0);
    }

    private static class SchemaCache<T>
    {
        public static readonly Schema Default = GetSchemaFromType<T>();
    }

    internal static class StructBuilderHelper
    {
        // =================================================================
        // Columnar Construction
        // =================================================================
        public static StructArray BuildStructArray<T>(IEnumerable<T> data)
        {
            IList<T> dataList = data as IList<T> ?? data.ToList();
            int length = dataList.Count;
            Type type = typeof(T);

            // Direct Reflection to ensure exact names and internal props
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            List<Field> fields = new();
            List<IArrowArray> childrenArrays = new();

            foreach (PropertyInfo prop in properties)
            {
                Type memberType = prop.PropertyType;
                Func<T, object?> getter = CompileGetter<T>(prop);

                // Recursively build column
                IArrowArray childArray = ProjectAndBuild(dataList, memberType, getter);

                // [FIX] Use prop.Name directly (Case Sensitive)
                Field finalField = new(prop.Name, childArray.Data.DataType, true);

                fields.Add(finalField);
                childrenArrays.Add(childArray);
            }

            // Build Validity Bitmap
            ArrowBuffer.BitmapBuilder validityBuilder = new();
            int nullCount = 0;
            foreach (T item in dataList)
                if (item == null)
                {
                    validityBuilder.Append(false);
                    nullCount++;
                }
                else
                {
                    validityBuilder.Append(true);
                }

            // [FIX] Removed .ValueBuffer
            ArrowBuffer validityBuffer = validityBuilder.Build();

            StructType structType = new(fields);

            return new StructArray(
                structType,
                length,
                childrenArrays,
                validityBuffer,
                nullCount
            );
        }

        // =================================================================
        // Helper：Reflection Bridge
        // =================================================================

        private static IArrowArray ProjectAndBuild<TParent>(
            IList<TParent> data,
            Type propType,
            Func<TParent, object?> getter)
        {
            Type cleanType = Nullable.GetUnderlyingType(propType) ?? propType;
            Type targetType = cleanType.IsValueType ? typeof(Nullable<>).MakeGenericType(cleanType) : cleanType;

            MethodInfo method = typeof(StructBuilderHelper)
                .GetMethod(nameof(BuildColumn), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(typeof(TParent), targetType);

            return (IArrowArray)method.Invoke(null, [data, getter])!;
        }

        private static IArrowArray BuildColumn<TParent, TProp>(IList<TParent> data, Func<TParent, object?> getter)
        {
            List<TProp> columnData = new(data.Count);
            foreach (TParent item in data)
            {
                if (item == null)
                {
                    columnData.Add(default!);
                    continue;
                }

                object? rawVal = getter(item);
                if (rawVal == null) columnData.Add(default!);
                else columnData.Add((TProp)rawVal);
            }

            // Call main Build with strong type TProp
            return Build(columnData);
        }

        // =================================================================
        // Compile Expression Tree for Getter
        // =================================================================
        private static Func<T, object?> CompileGetter<T>(PropertyInfo prop)
        {
            ParameterExpression instanceParam = Expression.Parameter(typeof(T), "item");
            MemberExpression memberAccess = Expression.Property(instanceParam, prop);
            UnaryExpression convertToObject = Expression.Convert(memberAccess, typeof(object));
            return Expression.Lambda<Func<T, object?>>(convertToObject, instanceParam).Compile();
        }
    }
}