using System.Data.SqlTypes;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Apache.Arrow.Types;
using AwesomeAssertions;
using Iceberg.Net.Schemas;
using Schema = Iceberg.Net.Schemas.Schema;

namespace Iceberg.Net.Tests;

public class DecimalTests
{
    [Fact]
    public void DecimalWithControlsArrowAndIcebergSchemas()
    {
        Decimal128Type arrowType = DecimalRow.ArrowSchema.GetFieldByName(nameof(DecimalRow.Wide)).DataType
            .Should().BeOfType<Decimal128Type>().Subject;
        arrowType.Precision.Should().Be(38);
        arrowType.Scale.Should().Be(18);

        Schema icebergSchema = CSharpSchema.ToIcebergSchema(typeof(DecimalRow), null, _ => 1);
        icebergSchema.Fields.Single(field => field.Name == nameof(DecimalRow.Wide)).FieldType
            .Should().Be(new PrimitiveType.Decimal(38, 18));
        icebergSchema.Fields.Single(field => field.Name == nameof(DecimalRow.Compact)).FieldType
            .Should().Be(new PrimitiveType.Decimal(9, 2));
    }

    [Fact]
    public void SqlDecimalRoundTripsWithoutLosingPrecision()
    {
        DecimalRow[] expected = DecimalRow.TestRows();

        using RecordBatch batch = RecordBatchBuilder.FromObjects(expected);
        IReadOnlyList<DecimalRow> actual = DecimalRow.ListFromRecordBatch(batch);

        actual.Select(row => row.Wide?.ToString()).Should()
            .Equal(expected.Select(row => row.Wide?.ToString()));
        actual.Select(row => row.Compact.ToString()).Should()
            .Equal(expected.Select(row => row.Compact.ToString()));
    }

    [Fact]
    public void ClrDecimalIsNotSupported()
    {
        Action buildArrow = () => RecordBatchBuilder.FromObjects([new ClrDecimalRow { Value = 1m }]);
        Action buildIceberg = () => CSharpSchema.ToIcebergSchema(typeof(ClrDecimalRow), null, _ => 1);

        buildArrow.Should().Throw<NotSupportedException>().WithMessage("*Use SqlDecimal*");
        buildIceberg.Should().Throw<NotSupportedException>().WithMessage("*Use SqlDecimal*");
    }

    private sealed class ClrDecimalRow
    {
        public required decimal Value { get; init; }
    }
}

[ArrowSerializable]
public partial record DecimalRow
{
    [DecimalWith(38, 18)]
    public required SqlDecimal? Wide { get; init; }

    [DecimalWith(9, 2)]
    public required SqlDecimal Compact { get; init; }

    public static DecimalRow[] TestRows() =>
    [
        new()
        {
            Wide = SqlDecimal.Parse("99999999999999999999.123456789012345678"),
            Compact = SqlDecimal.Parse("1234567.89")
        },
        new() { Wide = null, Compact = SqlDecimal.Parse("-1234567.89") }
    ];
}
