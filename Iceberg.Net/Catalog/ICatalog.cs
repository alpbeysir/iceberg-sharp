using Iceberg.Net.Rest;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Catalog;

public interface ICatalog : IDisposable
{
    public IReadOnlyDictionary<string, string> ObjectStorageProperties { get; }

    public async Task<Table> CreateTableAsync(
        Identifier identifier,
        Schemas.Schema schema,
        CancellationToken cancellationToken = default)
    {
        return await CreateTableInternalAsync(identifier, schema, false, cancellationToken);
    }

    internal Task<Table> CreateTableInternalAsync(
        Identifier identifier,
        Schemas.Schema schema,
        bool stage = false,
        CancellationToken cancellationToken = default);

    Task<Table> UpdateTableAsync(
        Identifier identifier,
        List<ITableUpdate> updates,
        List<ITableRequirement> requirements,
        CancellationToken cancellationToken = default);

    Task<Table> LoadTableAsync(
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
