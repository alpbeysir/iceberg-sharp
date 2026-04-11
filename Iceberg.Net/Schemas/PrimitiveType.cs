using System.Text.RegularExpressions;

namespace Iceberg.Net.Schemas;

public partial record PrimitiveType(string Name) : IIcebergType
{
    public static PrimitiveType Parse(string name)
    {
        return name switch
        {
            "boolean" => new Boolean(),
            "int" => new Int(),
            "long" => new Long(),
            "float" => new Float(),
            "double" => new Double(),
            "date" => new Date(),
            "time" => new Time(),
            "timestamp" => new Timestamp(),
            "timestamptz" => new TimestampTz(),
            "timestamp_ns" => new TimestampNs(),
            "timestamptz_ns" => new TimestampTzNs(),
            "string" => new String(),
            "uuid" => new Uuid(),
            "binary" => new Binary(),
            _ when name.StartsWith("decimal") => ParseDecimal(name),
            _ when name.StartsWith("fixed") => ParseFixed(name),
            _ => throw new ArgumentException($"Unknown Iceberg type: {name}")
        };
    }

    private static Decimal ParseDecimal(string name)
    {
        var match = DecimalRegex().Match(name);
        return match.Success
            ? new Decimal(int.Parse(match.Groups[1].Value), int.Parse(match.Groups[2].Value))
            : throw new ArgumentException($"Invalid decimal: {name}");
    }

    private static Fixed ParseFixed(string name)
    {
        var match = FixedRegex().Match(name);
        return match.Success
            ? new Fixed(int.Parse(match.Groups[1].Value))
            : throw new ArgumentException($"Invalid fixed: {name}");
    }

    [GeneratedRegex(@"decimal\((\d+),\s*(\d+)\)")]
    private static partial Regex DecimalRegex();

    [GeneratedRegex(@"fixed\[(\d+)\]")]
    private static partial Regex FixedRegex();

    // --- Subtypes ---

    public sealed record Boolean() : PrimitiveType("boolean");

    public sealed record Int() : PrimitiveType("int");

    public sealed record Long() : PrimitiveType("long");

    public sealed record Float() : PrimitiveType("float");

    public sealed record Double() : PrimitiveType("double");

    public sealed record Date() : PrimitiveType("date");

    public sealed record Time() : PrimitiveType("time");

    public sealed record Timestamp() : PrimitiveType("timestamp");

    public sealed record TimestampTz() : PrimitiveType("timestamptz");

    public sealed record TimestampNs() : PrimitiveType("timestamp_ns");

    public sealed record TimestampTzNs() : PrimitiveType("timestamptz_ns");

    public sealed record String() : PrimitiveType("string");

    public sealed record Uuid() : PrimitiveType("uuid");

    public sealed record Binary() : PrimitiveType("binary");

    public sealed record Decimal(int P, int S) : PrimitiveType($"decimal({P}, {S})");

    public sealed record Fixed(int L) : PrimitiveType($"fixed[{L}]");
}