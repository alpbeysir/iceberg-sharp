using Iceberg.Net.Catalog;
using Iceberg.Net.Tests;

namespace Iceberg.Net.Spark.Tests;

public sealed class SparkRestCatalogFixture : RestCatalogFixture
{
    public override Identifier BaseNamespace => ["test-spark"];
}
