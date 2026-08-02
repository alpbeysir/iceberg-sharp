using System.Collections;
using System.Data.SqlTypes;
using System.Reflection;

namespace Iceberg.Net.Tests;

public static class TestRowNormalizer
{
    public static object? ForDuckDb(object? value) => Normalize(value, false, false);

    public static object? ForPySpark(object? value) => Normalize(value, true, true);

    private static object? Normalize(object? value, bool integralAsInt64, bool unspecifiedDateTime)
    {
        if (value is null or DBNull) return null;
        if (value is SqlDecimal sqlDecimal)
            return sqlDecimal.IsNull ? null : sqlDecimal.Value;
        if (value is decimal decimalValue) return decimalValue;
        if (value is DateTime dateTime)
            return new DateTime(
                dateTime.Ticks / TimeSpan.TicksPerMicrosecond * TimeSpan.TicksPerMicrosecond,
                unspecifiedDateTime ? DateTimeKind.Unspecified : dateTime.Kind);
        if (value is DateTimeOffset dateTimeOffset)
            return new DateTimeOffset(
                dateTimeOffset.Ticks / TimeSpan.TicksPerMicrosecond * TimeSpan.TicksPerMicrosecond,
                dateTimeOffset.Offset);
        if (value is byte[] bytes) return bytes;
        if (value is Stream stream)
        {
            long position = stream.CanSeek ? stream.Position : 0;
            using MemoryStream copy = new();
            stream.CopyTo(copy);
            if (stream.CanSeek) stream.Position = position;
            return copy.ToArray();
        }

        if (integralAsInt64 && value is byte or sbyte or short or ushort or int or uint or long)
            return Convert.ToInt64(value);
        if (value is string or bool or byte or sbyte or short or ushort or int or uint or long or ulong
            or float or double or DateOnly or TimeOnly or Guid)
            return value;

        if (value is IDictionary dictionary)
        {
            var normalized = new Dictionary<object, object?>();
            foreach (DictionaryEntry entry in dictionary)
                normalized[Normalize(entry.Key, integralAsInt64, unspecifiedDateTime)!] =
                    Normalize(entry.Value, integralAsInt64, unspecifiedDateTime);
            return normalized;
        }

        if (value is IEnumerable enumerable)
            return enumerable.Cast<object?>()
                .Select(item => Normalize(item, integralAsInt64, unspecifiedDateTime))
                .ToList();

        PropertyInfo[] properties = value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.CanRead && property.GetIndexParameters().Length == 0)
            .ToArray();
        if (properties.Length == 0) return value;

        var result = new Dictionary<object, object?>();
        foreach (PropertyInfo property in properties)
            result[property.Name] = Normalize(
                property.GetValue(value),
                integralAsInt64,
                unspecifiedDateTime);
        return result;
    }
}
