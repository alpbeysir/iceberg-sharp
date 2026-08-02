using System.Collections.Immutable;
using System.Numerics;
using System.Threading.Channels;
using EngineeredWood.Expressions;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Spark.Tests;

public partial class PySparkTests
{
    [Fact]
    public async Task AllSparkPrimitivePartitionTypesAreReadAsLiterals()
    {
        // These are all primitive Iceberg types that Spark can create. Spark has no Iceberg
        // mapping for UUID, fixed, or nanosecond timestamps, and Iceberg's Spark 4.1 runtime
        // rejects Spark's TimeType as unsupported.
        Identifier identifier = GetTableName();
        string[] expectedNames =
        [
            "boolean_value",
            "int_value",
            "long_value",
            "float_value",
            "double_value",
            "decimal_value",
            "date_value",
            "timestamp_value",
            "timestamptz_value",
            "string_value",
            "binary_value"
        ];
        string[] expectedTypeNames =
        [
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "decimal(38, 18)",
            "date",
            "timestamp",
            "timestamptz",
            "string",
            "binary"
        ];
        ImmutableArray<LiteralValue?> expectedPartition =
        [
            LiteralValue.Of(true),
            LiteralValue.Of(34),
            LiteralValue.Of(1234567890123L),
            LiteralValue.Of(1.25f),
            LiteralValue.Of(-2.5),
            LiteralValue.HighPrecisionDecimalOf(
                BigInteger.Parse("12345678901234567890123456789012345678"),
                18),
            LiteralValue.Of(new DateOnly(2024, 2, 29)),
            LiteralValue.Of(
                new DateTimeOffset(2024, 2, 29, 12, 34, 56, TimeSpan.Zero).AddTicks(1_234_560)),
            LiteralValue.Of(
                new DateTimeOffset(2024, 2, 29, 12, 34, 56, TimeSpan.Zero).AddTicks(6_543_210)),
            LiteralValue.Of("iceberg"),
            LiteralValue.Of(new byte[] { 0, 1, 2, 255 })
        ];

        await pySparkFixture.WriteAllPrimitiveTypesPartitionedTable(identifier);

        Table table = await Catalog.LoadTableAsync(
                          identifier,
                          cancellationToken: TestContext.Current.CancellationToken) ??
                      throw new InvalidOperationException($"Table '{identifier}' was not found.");
        PartitionSpec spec = Assert.Single(table.Metadata.PartitionSpecs);
        Assert.Equal(expectedNames, spec.Fields.Select(field => field.Name));

        Schema schema = table.Metadata.SchemasById[table.Metadata.CurrentSchemaId!.Value];
        Assert.Equal(
            expectedTypeNames,
            schema.Fields
                .Where(field => expectedNames.Contains(field.Name))
                .Select(field => Assert.IsType<PrimitiveType>(field.FieldType).Name));

        List<DataFile> dataFiles = await ReadActiveDataFiles(table);
        Assert.NotEmpty(dataFiles);
        Assert.All(dataFiles, file => Assert.Equal(expectedPartition.Length, file.Partition.Length));
        Assert.Contains(dataFiles, file => file.Partition.All(value => value is null));
        Assert.Contains(dataFiles, file => expectedPartition.SequenceEqual(file.Partition));
        Assert.All(
            dataFiles,
            file => Assert.True(
                file.Partition.All(value => value is null) ||
                expectedPartition.SequenceEqual(file.Partition),
                $"Unexpected partition values in '{file.FilePath}'."));
    }

    private static async Task<List<DataFile>> ReadActiveDataFiles(Table table)
    {
        Channel<ManifestEntry> entries = Channel.CreateUnbounded<ManifestEntry>();
        await table.Operations().ReadManifests(
            entries.Writer,
            cancellationToken: TestContext.Current.CancellationToken);
        entries.Writer.Complete();

        List<DataFile> dataFiles = [];
        while (entries.Reader.TryRead(out ManifestEntry entry))
            if (entry.Status != Status.Deleted)
                dataFiles.Add(entry.DataFile);
        return dataFiles;
    }
}
