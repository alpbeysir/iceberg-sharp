using Iceberg.Net.Rest;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Iceberg.Net.Catalog;

public interface ICatalog : IDisposable
{
    ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;

    string? Resolve(string key);

    async Task<TableOperations> OperationsAsync(
        Identifier identifier,
        CancellationToken cancellationToken = default)
    {
        Table? table = await LoadTableAsync(
            identifier,
            cancellationToken: cancellationToken);
        return table?.Operations() ?? new TableOperations(identifier, this);
    }

    public async Task<Table> CreateTableAsync(
        Identifier identifier,
        Schemas.Schema schema,
        CancellationToken cancellationToken = default)
    {
        return await CreateTableAsync(identifier, schema, false, cancellationToken);
    }

    public async Task<Table> CreateTableAsync(
        Identifier identifier,
        Schemas.Schema schema,
        IReadOnlyDictionary<string, string> properties,
        CancellationToken cancellationToken = default)
    {
        return await CreateTableAsync(identifier, schema, properties, false, cancellationToken);
    }

    Task<Table> CreateTableAsync(
        Identifier identifier,
        Schemas.Schema schema,
        bool stage,
        CancellationToken cancellationToken = default);

    public async Task<Table> CreateTableAsync(
        Identifier identifier,
        Schemas.Schema schema,
        IReadOnlyDictionary<string, string>? properties,
        bool stage,
        CancellationToken cancellationToken = default)
    {
        if (properties is { Count: > 0 })
            throw new NotSupportedException(
                $"Catalog '{GetType().Name}' does not support table creation properties.");

        return await CreateTableAsync(identifier, schema, stage, cancellationToken);
    }

    Task<Table> UpdateTableAsync(
        Table table,
        List<ITableUpdate> updates,
        List<ITableRequirement> requirements,
        CancellationToken cancellationToken = default);

    Task<Table?> LoadTableAsync(
        Identifier identifier,
        Snapshots snapshots = Snapshots.All,
        CancellationToken cancellationToken = default);

    Namespace GetNamespace(Identifier identifier, CancellationToken cancellationToken = default);

    Task CreateNamespaceAsync(
        Identifier identifier,
        Dictionary<string, string>? properties = null,
        CancellationToken cancellationToken = default);

    Task<bool> NamespaceExistsAsync(Identifier identifier, CancellationToken cancellationToken = default);

    Task DropNamespaceAsync(
        Identifier identifier,
        bool recursive = false,
        bool purgeData = false,
        CancellationToken cancellationToken = default);

    Task CreateNamespaceIfNotExistsAsync(
        Identifier identifier,
        Dictionary<string, string>? properties = null,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<Namespace> ListNamespacesAsync(
        Identifier? parent,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<Table> ListTablesAsync(
        Identifier ns,
        CancellationToken cancellationToken = default);


    Task<INode> GenerateTree(Identifier? parent = null, CancellationToken cancellationToken = default);
}
