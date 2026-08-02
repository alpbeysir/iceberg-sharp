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

using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using System.Data.SqlTypes;

namespace Apache.Arrow.Serialization;

/// <summary>
///     Utility methods for building Arrow arrays in generated code.
/// </summary>
public static class ArrowArrayHelper
{
    /// <summary>
    ///     Creates an array of the specified Arrow type with all null values.
    ///     Used by generated code for nullable nested record fields.
    /// </summary>
    public static IArrowArray BuildNullArray(IArrowType type, int length)
    {
        switch (type)
        {
            case BooleanType:
            {
                BooleanArray.Builder b = new BooleanArray.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            // case Bool8Type:
            // {
            //     var b = new Bool8Array.Builder();
            //     for (int i = 0; i < length; i++) b.AppendNull();
            //     return b.Build();
            // }
            case Int8Type:
            {
                Int8Array.Builder b = new Int8Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case UInt8Type:
            {
                UInt8Array.Builder b = new UInt8Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case Int16Type:
            {
                Int16Array.Builder b = new Int16Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case UInt16Type:
            {
                UInt16Array.Builder b = new UInt16Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case Int32Type:
            {
                Int32Array.Builder b = new Int32Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case UInt32Type:
            {
                UInt32Array.Builder b = new UInt32Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case Int64Type:
            {
                Int64Array.Builder b = new Int64Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case UInt64Type:
            {
                UInt64Array.Builder b = new UInt64Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case HalfFloatType:
            {
                HalfFloatArray.Builder b = new HalfFloatArray.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case FloatType:
            {
                FloatArray.Builder b = new FloatArray.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case DoubleType:
            {
                DoubleArray.Builder b = new DoubleArray.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case Decimal128Type dt:
            {
                Decimal128Array.Builder b = new Decimal128Array.Builder(dt);
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case StringType:
            {
                StringArray.Builder b = new StringArray.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case BinaryType:
            {
                BinaryArray.Builder b = new BinaryArray.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case StringViewType:
            {
                StringViewArray.Builder b = new StringViewArray.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case BinaryViewType:
            {
                BinaryViewArray.Builder b = new BinaryViewArray.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            // case GuidType:
            // {
            //     var b = new GuidArray.Builder();
            //     for (var i = 0; i < length; i++) b.AppendNull();
            //     return b.Build();
            // }
            case FixedSizeBinaryType fbt:
            {
                return BuildNullFixedSizeBinaryArray(fbt, length);
            }
            case Date32Type:
            {
                Date32Array.Builder b = new Date32Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case Date64Type:
            {
                Date64Array.Builder b = new Date64Array.Builder();
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case TimestampType tsType:
            {
                TimestampArray.Builder b = new TimestampArray.Builder(tsType);
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case Time32Type t32:
            {
                Time32Array.Builder b = new Time32Array.Builder(t32);
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case Time64Type t64:
            {
                Time64Array.Builder b = new Time64Array.Builder(t64);
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case DurationType dur:
            {
                DurationArray.Builder b = new DurationArray.Builder(dur);
                for (int i = 0; i < length; i++) b.AppendNull();
                return b.Build();
            }
            case DictionaryType dt:
            {
                Int16Array.Builder idx = new Int16Array.Builder();
                for (int i = 0; i < length; i++) idx.AppendNull();
                StringArray dict = new StringArray.Builder().Build();
                return new DictionaryArray(dt, idx.Build(), dict);
            }
            case StructType st:
            {
                var children = new IArrowArray[st.Fields.Count];
                for (int i = 0; i < children.Length; i++)
                    children[i] = BuildNullArray(st.Fields[i].DataType, length);
                ArrowBuffer.BitmapBuilder bitmapBuilder = new ArrowBuffer.BitmapBuilder();
                for (int i = 0; i < length; i++) bitmapBuilder.Append(false);
                return new StructArray(st, length, children, bitmapBuilder.Build(), length);
            }
            case ListType lt:
            {
                ListArray.Builder lb = new ListArray.Builder(lt.ValueDataType);
                for (int i = 0; i < length; i++) lb.AppendNull();
                return lb.Build();
            }
            case MapType mt:
            {
                MapArray.Builder mb = new MapArray.Builder(mt);
                for (int i = 0; i < length; i++) mb.AppendNull();
                return mb.Build();
            }
            default:
                return new NullArray(length);
        }
    }

    // --- FixedSizeBinary helpers (no concrete Builder class in Arrow C#) ---

    private static FixedSizeBinaryArray BuildNullFixedSizeBinaryArray(FixedSizeBinaryType type, int length)
    {
        byte[] valueBytes = new byte[length * type.ByteWidth];
        ArrowBuffer.BitmapBuilder validityBuffer = new ArrowBuffer.BitmapBuilder();
        for (int i = 0; i < length; i++)
            validityBuffer.Append(false);
        ArrayData data = new ArrayData(
            type,
            length,
            length,
            0,
            new[] { validityBuffer.Build(), new ArrowBuffer(valueBytes) });
        return new FixedSizeBinaryArray(data);
    }

    // public static IArrowArray BuildGuidArray(Guid value)
    // {
    //     var b = new GuidArray.Builder();
    //     b.Append(value);
    //     return b.Build();
    // }

    // public static IArrowArray BuildGuidArray(Guid? value)
    // {
    //     var b = new GuidArray.Builder();
    //     if (value is { } v)
    //         b.Append(v);
    //     else
    //         b.AppendNull();
    //     return b.Build();
    // }

    // public static IArrowArray BuildGuidArray<T>(IReadOnlyList<T> items)
    // {
    //     var b = new GuidArray.Builder();
    //     for (var i = 0; i < items.Count; i++)
    //         if (items[i] is Guid v)
    //             b.Append(v);
    //         else
    //             b.AppendNull();
    //     return b.Build();
    // }

    // --- TimeOnly helpers (Time64) ---

    public static IArrowArray BuildTimeOnlyArray(TimeOnly value)
    {
        Time64Array.Builder b = new Time64Array.Builder(Time64Type.Default);
        b.Append(value);
        return b.Build();
    }

    public static IArrowArray BuildTimeOnlyArray(TimeOnly? value)
    {
        Time64Array.Builder b = new Time64Array.Builder(Time64Type.Default);
        if (value is { } v)
            b.Append(v);
        else
            b.AppendNull();
        return b.Build();
    }

    public static IArrowArray BuildTimeOnlyArray<T>(IReadOnlyList<T> items)
    {
        Time64Array.Builder b = new Time64Array.Builder(Time64Type.Default);
        foreach (T item in items)
            if (item is TimeOnly v)
                b.Append(v);
            else
                b.AppendNull();
        return b.Build();
    }

    public static TimeOnly ReadTimeOnly(Time64Array array, int index)
    {
        return array.GetTime(index)!.Value;
    }

    // --- TimeSpan helpers (Duration) ---

    public static IArrowArray BuildDurationArray(TimeSpan value)
    {
        DurationArray.Builder b = new DurationArray.Builder(DurationType.Microsecond);
        b.Append(value);
        return b.Build();
    }

    public static IArrowArray BuildDurationArray(TimeSpan? value)
    {
        DurationArray.Builder b = new DurationArray.Builder(DurationType.Microsecond);
        if (value is { } v)
            b.Append(v);
        else
            b.AppendNull();
        return b.Build();
    }

    public static IArrowArray BuildDurationArray<T>(IReadOnlyList<T> items)
    {
        DurationArray.Builder b = new DurationArray.Builder(DurationType.Microsecond);
        foreach (T item in items)
            if (item is TimeSpan v)
                b.Append(v);
            else
                b.AppendNull();
        return b.Build();
    }

    public static TimeSpan ReadDuration(DurationArray array, int index)
    {
        return array.GetTimeSpan(index)!.Value;
    }

    // --- Decimal helpers (Decimal128) ---

    public static IArrowArray BuildDecimalArray(SqlDecimal value, Decimal128Type type)
    {
        return new Decimal128Array.Builder(type).Append(value).Build();
    }

    public static IArrowArray BuildDecimalArray(SqlDecimal? value, Decimal128Type type)
    {
        Decimal128Array.Builder b = new Decimal128Array.Builder(type);
        if (value is { } v)
            b.Append(v);
        else
            b.AppendNull();
        return b.Build();
    }

    public static IArrowArray BuildDecimalArray<T>(IReadOnlyList<T> items, Decimal128Type type)
    {
        Decimal128Array.Builder b = new Decimal128Array.Builder(type);
        foreach (T item in items)
        {
            switch (item)
            {
                case SqlDecimal value:
                    b.Append(value);
                    break;
                default:
                    b.AppendNull();
                    break;
            }
        }
        return b.Build();
    }

    // --- UTC normalization helpers ---

    /// <summary>
    ///     Converts a DateTime to a UTC DateTimeOffset.
    ///     Local/Unspecified kinds are converted via ToUniversalTime(); Utc is wrapped directly.
    /// </summary>
    public static DateTimeOffset ToUtcDateTimeOffset(DateTime value)
    {
        if (value.Kind == DateTimeKind.Utc)
            return new DateTimeOffset(value, TimeSpan.Zero);
        return new DateTimeOffset(value.ToUniversalTime(), TimeSpan.Zero);
    }

    /// <summary>
    ///     Normalizes a DateTimeOffset to UTC.
    /// </summary>
    public static DateTimeOffset ToUtcDateTimeOffset(DateTimeOffset value)
    {
        return value.ToUniversalTime();
    }

    /// <summary>
    ///     Converts a DateTime to a wall-clock DateTimeOffset (no timezone conversion).
    ///     The raw ticks are preserved regardless of DateTimeKind.
    /// </summary>
    public static DateTimeOffset ToWallClockDateTimeOffset(DateTime value)
    {
        return new DateTimeOffset(value.Ticks, TimeSpan.Zero);
    }

    /// <summary>
    ///     Converts a DateTimeOffset to a wall-clock DateTimeOffset (strips timezone offset).
    ///     The wall-clock time (DateTime) is preserved, offset is discarded.
    /// </summary>
    public static DateTimeOffset ToWallClockDateTimeOffset(DateTimeOffset value)
    {
        return new DateTimeOffset(value.DateTime.Ticks, TimeSpan.Zero);
    }
}
