using Iceberg.Net.Schemas;

namespace Iceberg.Net.Tests;

public class IcebergTypeComparerTests
{
    private readonly IcebergTypeComparer _comparer = new();

    [Fact]
    public void DeserializedAndConcretePrimitivesAreEqual()
    {
        PrimitiveType deserialized = new("decimal(20,18)");
        PrimitiveType concrete = new PrimitiveType.Decimal(20, 18);

        Assert.True(_comparer.Equals(deserialized, concrete));
        Assert.Equal(_comparer.GetHashCode(deserialized), _comparer.GetHashCode(concrete));
    }

    [Fact]
    public void FieldIdsDoNotAffectTypeEquality()
    {
        Schema left = new(
        [
            new StructField(
                1,
                "values",
                new ListType(2, new PrimitiveType.String(), true),
                true),
            new StructField(
                3,
                "lookup",
                new MapType(4, new PrimitiveType.String(), 5, new PrimitiveType.Long(), false),
                true)
        ]);
        Schema right = new(
        [
            new StructField(
                101,
                "values",
                new ListType(102, new PrimitiveType("string"), true),
                true),
            new StructField(
                103,
                "lookup",
                new MapType(104, new PrimitiveType("string"), 105, new PrimitiveType("long"), false),
                true)
        ]);

        Assert.True(_comparer.Equals(left, right));
        Assert.Equal(_comparer.GetHashCode(left), _comparer.GetHashCode(right));
    }

    [Fact]
    public void RequirednessAndTypesAffectEquality()
    {
        Schema expected = new([new StructField(1, "value", new PrimitiveType.String(), true)]);
        Schema optional = new([new StructField(1, "value", new PrimitiveType.String(), false)]);
        Schema differentType = new([new StructField(1, "value", new PrimitiveType.Long(), true)]);

        Assert.False(_comparer.Equals(expected, optional));
        Assert.False(_comparer.Equals(expected, differentType));
    }
}
