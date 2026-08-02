using System.Collections;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Reflection;
using Apache.Arrow.Serialization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Tests.DataGeneration;

namespace Iceberg.Net.Tests;

public class DuckDBTests(RestCatalogFixture restFixture, DuckDbFixture duckDbFixture)
    : TableTest(restFixture), IClassFixture<DuckDbFixture>
{
    [Theory]
    [AutoIcebergData]
    public async Task SimpleNesting(List<MyNested> rows)
    {
        await Run(rows);
    }

    [Theory]
    [AutoIcebergData]
    public async Task Simple(List<MySimpleRow> rows)
    {
        await Run(rows);
    }

    [Theory]
    [AutoIcebergData]
    public async Task NestedComplexRow(List<NestedComplexRow> rows)
    {
        await Run(rows);
    }

    [Theory]
    [AutoIcebergData]
    public async Task ManyTypes(List<ManyTypes> rows)
    {
        await Run(rows);
    }

    [Fact]
    public void DecimalNormalizationUsesNumericValue()
    {
        object? expected = Normalize(SqlDecimal.Parse("93282.20"));
        object? actual = Normalize(93282.2m);

        Assert.Equivalent(expected, actual, strict: true);
    }

    // [Theory]
    // [AutoIcebergData]
    // public async Task DeepNesting(List<MyDeeplyNestedComplexRow> rows)
    // {
    //     await Run(rows);
    // }

    private async Task Run<T>(List<T> original) where T : IArrowSerializer<T>
    {
        Identifier identifier = await Write(original);
        await Verify(identifier, original);
        await using DbDataReader reader =
            await duckDbFixture.DuckDbCatalog
                .ExecuteQuery($"SELECT * FROM {duckDbFixture.DuckDbCatalog.CatalogName}.{identifier};");

        var actual = new List<object?>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var row = new Dictionary<object, object?>();
            for (int column = 0; column < reader.FieldCount; column++)
                row[reader.GetName(column)] = Normalize(reader.GetValue(column));
            actual.Add(row);
        }

        List<object?> expected = original.Select(row => Normalize(row)).ToList();
        Assert.Equivalent(expected, actual, strict: true);
    }

    private static object? Normalize(object? value)
    {
        if (value is null or DBNull) return null;
        if (value is SqlDecimal sqlDecimal) return sqlDecimal.IsNull ? null : sqlDecimal.Value;
        if (value is decimal decimalValue) return decimalValue;
        if (value is DateTime dateTime)
            return new DateTime(
                dateTime.Ticks / TimeSpan.TicksPerMicrosecond * TimeSpan.TicksPerMicrosecond,
                dateTime.Kind);
        if (value is DateTimeOffset dateTimeOffset)
            return new DateTimeOffset(
                dateTimeOffset.Ticks / TimeSpan.TicksPerMicrosecond * TimeSpan.TicksPerMicrosecond,
                dateTimeOffset.Offset);
        if (value is byte[] bytes) return bytes;
        if (value is Stream stream)
        {
            long position = stream.CanSeek ? stream.Position : 0;
            using MemoryStream copy = new MemoryStream();
            stream.CopyTo(copy);
            if (stream.CanSeek) stream.Position = position;
            return copy.ToArray();
        }
        if (value is string or bool or byte or sbyte or short or ushort or int or uint or long or ulong
            or float or double or DateOnly or TimeOnly or Guid)
            return value;

        if (value is IDictionary dictionary)
        {
            var normalized = new Dictionary<object, object?>();
            foreach (DictionaryEntry entry in dictionary)
                normalized[Normalize(entry.Key)!] = Normalize(entry.Value);
            return normalized;
        }

        if (value is IEnumerable enumerable)
            return enumerable.Cast<object?>().Select(Normalize).ToList();

        PropertyInfo[] properties = value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.CanRead && property.GetIndexParameters().Length == 0)
            .ToArray();
        if (properties.Length == 0) return value;

        var result = new Dictionary<object, object?>();
        foreach (PropertyInfo property in properties)
            result[property.Name] = Normalize(property.GetValue(value));
        return result;
    }

}
