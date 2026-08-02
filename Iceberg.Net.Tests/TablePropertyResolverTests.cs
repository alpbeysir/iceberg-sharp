using System.Globalization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Tests;

public class TablePropertyResolverTests
{
    [Fact]
    public void ResolvesTypedValuesAndDefaultsUsingInvariantCulture()
    {
        TablePropertyResolver properties = new(
            new Dictionary<string, string>
            {
                ["boolean"] = "true",
                ["int"] = "123",
                ["long"] = "9876543210",
                ["double"] = "1.25"
            });
        CultureInfo originalCulture = CultureInfo.CurrentCulture;

        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");

            Assert.True(properties.GetBoolean("boolean", false));
            Assert.Equal(123, properties.GetInt32("int", 0));
            Assert.Equal(9876543210, properties.GetInt64("long", 0));
            Assert.Equal(1.25, properties.GetDouble("double", 0));
            Assert.Equal("default", properties.GetString("missing", "default"));
            Assert.Equal(42, properties.GetInt32("missing", 42));
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
        }
    }

    [Theory]
    [InlineData("boolean", "yes", "boolean")]
    [InlineData("int", "1.2", "32-bit integer")]
    [InlineData("long", "not-a-number", "64-bit integer")]
    [InlineData("double", "1,25", "number")]
    public void InvalidTypedValuesIdentifyThePropertyAndValue(
        string property,
        string value,
        string expectedType)
    {
        TablePropertyResolver properties = new(new Dictionary<string, string> { [property] = value });

        Exception exception = Assert.Throws<FormatException>(
            () => property switch
            {
                "boolean" => properties.GetBoolean(property, false),
                "int" => properties.GetInt32(property, 0) > 0,
                "long" => properties.GetInt64(property, 0) > 0,
                _ => properties.GetDouble(property, 0) > 0
            });

        Assert.Contains(property, exception.Message);
        Assert.Contains(value, exception.Message);
        Assert.Contains(expectedType, exception.Message);
    }

    [Fact]
    public void PrefixLookupReturnsTheSuffixWithoutChangingValues()
    {
        TablePropertyResolver properties = new(
            new Dictionary<string, string>
            {
                [TableProperties.ParquetColumnStatsEnabledPrefix + "customer.id"] = "false",
                ["unrelated"] = "true"
            });

        KeyValuePair<string, string> property = Assert.Single(
            properties.GetByPrefix(TableProperties.ParquetColumnStatsEnabledPrefix));

        Assert.Equal("customer.id", property.Key);
        Assert.Equal("false", property.Value);
    }

    [Fact]
    public void LocationOverridesAreAbsoluteAndNormalizedAsFolders()
    {
        Table table = CreateTable(
            "s3://warehouse/table",
            new Dictionary<string, string>
            {
                [TableProperties.WriteDataLocation] = "s3://data-bucket/custom-data",
                [TableProperties.WriteMetadataLocation] = "s3://metadata-bucket/custom-metadata/"
            });

        Assert.Equal("s3://data-bucket/custom-data/", table.DataFolderUri.AbsoluteUri);
        Assert.Equal("s3://metadata-bucket/custom-metadata/", table.MetadataFolderUri.AbsoluteUri);
    }

    [Fact]
    public void LocationsDefaultUnderTheTableRoot()
    {
        Table table = CreateTable("s3://warehouse/table", new Dictionary<string, string>());

        Assert.Equal("s3://warehouse/table/data/", table.DataFolderUri.AbsoluteUri);
        Assert.Equal("s3://warehouse/table/metadata/", table.MetadataFolderUri.AbsoluteUri);
    }

    [Fact]
    public void RelativeLocationOverridesAreRejected()
    {
        Table table = CreateTable(
            "s3://warehouse/table",
            new Dictionary<string, string> { [TableProperties.WriteDataLocation] = "custom-data" });

        FormatException exception = Assert.Throws<FormatException>(() => table.DataFolderUri);

        Assert.Contains(TableProperties.WriteDataLocation, exception.Message);
        Assert.Contains("custom-data", exception.Message);
    }

    private static Table CreateTable(
        string location,
        IReadOnlyDictionary<string, string> properties)
    {
        return new Table(
            new Identifier(["test", "table"]),
            null!,
            new TableMetadata
            {
                FormatVersion = 2,
                TableUuid = Guid.NewGuid().ToString(),
                Location = location,
                Properties = properties
            });
    }
}
