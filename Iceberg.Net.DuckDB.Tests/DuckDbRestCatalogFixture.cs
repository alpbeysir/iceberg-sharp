using Iceberg.Net.Catalog;
using Iceberg.Net.Tests;

namespace Iceberg.Net.DuckDB.Tests;

public sealed class DuckDbRestCatalogFixture : RestCatalogFixture
{
    public override Identifier BaseNamespace => ["test-duckdb"];
}
