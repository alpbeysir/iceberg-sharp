using AwesomeAssertions;
using Iceberg.Net.Catalog;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Tests;

public class CatalogTests(RestCatalogFixture fixture)
{
    private readonly ICatalog _catalog = fixture.GetCatalog();

    [Fact]
    public async Task TestCreateListDropNamespace()
    {
        Identifier identifier = [..fixture.BaseNamespace, "test_child"];
        await _catalog.CreateNamespaceAsync(identifier, cancellationToken: TestContext.Current.CancellationToken);
        var namespaces = _catalog.ListNamespacesAsync(fixture.BaseNamespace, TestContext.Current.CancellationToken);
        bool contains = await namespaces.AnyAsync(
            x => x.Identifier == identifier,
            TestContext.Current.CancellationToken);
        contains.Should().BeTrue();
        await _catalog.DropNamespaceAsync(identifier, cancellationToken: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task TestCreateTable()
    {
        Identifier identifier = [..fixture.BaseNamespace, "test_table"];
        Schema schema = new Schemas.Schema([]);
        await _catalog.CreateTableAsync(identifier, schema, TestContext.Current.CancellationToken);
        var namespaces = _catalog.ListTablesAsync(fixture.BaseNamespace, TestContext.Current.CancellationToken);
        bool contains = await namespaces.AnyAsync(
            x => x.Identifier == identifier,
            TestContext.Current.CancellationToken);
        contains.Should().BeTrue();
    }

    [Fact]
    public async Task TestListNamespacesTables()
    {
        await _catalog.GenerateTree(cancellationToken: TestContext.Current.CancellationToken);
    }
}