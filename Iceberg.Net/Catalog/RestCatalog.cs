using System.Net;
using System.Runtime.CompilerServices;
using Iceberg.Net.Rest;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Catalog;

public record UserConfig
{
    public readonly Dictionary<string, string> CatalogConfig = new();
    public readonly Dictionary<string, string> RequestHeaders = new();
    public required string BaseUrl;
    public IStorageConfig? StorageConfig;
    public string? Warehouse;
}

public record TypedCatalogConfig(CatalogConfig CatalogConfig, UserConfig UserConfig)
{
    public string? Prefix => Resolve("prefix");

    private string? Resolve(string key)
    {
        CatalogConfig.Defaults.TryGetValue(key, out var catalogDefault);
        UserConfig.CatalogConfig.TryGetValue(key, out var userOverride);
        CatalogConfig.Overrides.TryGetValue(key, out var catalogOverride);
        return catalogOverride ?? userOverride ?? catalogDefault;
    }
}

public sealed class RestCatalog : ICatalog
{
    private const int PageSize = 10;
    private readonly HttpClient _httpClient;

    private RestCatalog(
        RestCatalogClient apiClient,
        UserConfig userConfig,
        TypedCatalogConfig catalogConfig,
        HttpClient httpClient)
    {
        _httpClient = httpClient;
        ApiClient = apiClient;
        UserConfig = userConfig;
        CatalogConfig = catalogConfig;
    }

    private RestCatalogClient ApiClient { get; }
    private UserConfig UserConfig { get; }
    public TypedCatalogConfig CatalogConfig { get; }

    public IStorageConfig StorageConfig => UserConfig.StorageConfig ??
                                           throw new InvalidOperationException(
                                               "Storage credentials are not configured for the catalog");

    async Task<Table> ICatalog.CreateTableInternalAsync(
        Identifier identifier,
        Schemas.Schema schema,
        bool stage,
        CancellationToken cancellationToken)
    {
        var request = new CreateTableRequest
        {
            Name = identifier.GetTableName(),
            Schema = schema,
            Location = null,
            PartitionSpec = null,
            WriteOrder = null,
            StageCreate = stage,
            Properties = null
        };
        var response = await ApiClient.CreateTableAsync(
            request,
            identifier.GetParent().GetEncoded(),
            cancellationToken: cancellationToken);
        var table = new Table(identifier, this);
        table.Initialize(response.Metadata, response.StorageCredentials);
        return table;
    }

    public async Task<Table> UpdateTableAsync(
        Identifier identifier,
        List<ITableUpdate> updates,
        List<ITableRequirement> requirements,
        CancellationToken cancellationToken)
    {
        var response = await ApiClient.UpdateTableAsync(
            new CommitTableRequest
            {
                Identifier = identifier.ToTableIdentifier(),
                Requirements = requirements,
                Updates = updates
            },
            identifier.GetParent().GetEncoded(),
            identifier.GetTableName(),
            cancellationToken: cancellationToken);
        var table = new Table(identifier, this);
        table.Initialize(response.Metadata, null);
        return table;
    }

    public async Task<Table> LoadTableAsync(
        Identifier identifier,
        Snapshots snapshots = Snapshots.All,
        CancellationToken cancellationToken = default)
    {
        var response = await ApiClient.LoadTableAsync(
            identifier.GetParent().GetEncoded(),
            identifier.GetTableName(),
            snapshots: snapshots,
            cancellationToken: cancellationToken);
        var table = new Table(identifier, this);
        table.Initialize(response.Metadata, response.StorageCredentials);
        return table;
    }

    public Namespace GetNamespace(Identifier identifier, CancellationToken cancellationToken = default)
    {
        IAsyncEnumerable<INode> childNamespaces = ListNamespacesAsync(identifier, cancellationToken);
        IAsyncEnumerable<INode> childTables = ListTablesAsync(identifier, cancellationToken);
        return new Namespace(identifier, childNamespaces.Concat(childTables));
    }

    public async Task CreateNamespaceAsync(
        Identifier identifier,
        Dictionary<string, string>? properties = null,
        CancellationToken cancellationToken = default)
    {
        var ns = new Rest.Namespace();
        ns.AddRange(identifier);
        var request = new CreateNamespaceRequest(ns, properties ?? new Dictionary<string, string>());
        await ApiClient.CreateNamespaceAsync(request, null, cancellationToken);
    }

    public async Task<bool> NamespaceExistsAsync(Identifier identifier, CancellationToken cancellationToken = default)
    {
        try
        {
            await ApiClient.NamespaceExistsAsync(identifier.GetEncoded(), cancellationToken);
            return true;
        }
        catch (IcebergRestException exception)
        {
            if (exception.StatusCode == (int)HttpStatusCode.NotFound) return false;
            throw;
        }
    }


    public async Task CreateNamespaceIfNotExistsAsync(
        Identifier identifier,
        Dictionary<string, string>? properties = null,
        CancellationToken cancellationToken = default)
    {
        var ns = new Rest.Namespace();
        foreach (var part in identifier) ns.Add(part);
        var request = new CreateNamespaceRequest(ns, properties ?? new Dictionary<string, string>());
        try
        {
            await ApiClient.CreateNamespaceAsync(request, null, cancellationToken);
        }
        catch (IcebergRestException exception)
        {
            if (exception.StatusCode != 409) throw;
        }
    }

    public async IAsyncEnumerable<Namespace> ListNamespacesAsync(
        Identifier? parent,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string? pageToken = null;
        do
        {
            var resp =
                await ApiClient.ListNamespacesAsync(pageToken, PageSize, parent?.GetEncoded(), cancellationToken);
            foreach (var ns in resp.Namespaces)
            {
                var identifier = new Identifier(ns);
                yield return GetNamespace(identifier, cancellationToken);
            }

            pageToken = resp.NextPageToken;
        } while (pageToken != null);
    }

    public async IAsyncEnumerable<Table> ListTablesAsync(
        Identifier ns,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string? pageToken = null;
        do
        {
            var resp =
                await ApiClient.ListTablesAsync(ns.GetEncoded(), pageToken, PageSize, cancellationToken);
            foreach (var table in resp.Identifiers)
                yield return new Table(Identifier.FromTableIdentifier(table), this);

            pageToken = resp.NextPageToken;
        } while (pageToken != null);
    }

    public async Task<INode> GenerateTree(Identifier? parent = null, CancellationToken cancellationToken = default)
    {
        List<INode> children = [];
        var nsResponse = await ApiClient.ListNamespacesAsync(null, null, parent?.GetEncoded(), cancellationToken);
        foreach (var ns in nsResponse.Namespaces)
        {
            var child = await GenerateTree(new Identifier(ns), cancellationToken);
            children.Add(child);
        }

        if (parent is not null)
            children.AddRange(ListTablesAsync(parent.Value, cancellationToken).ToBlockingEnumerable(cancellationToken));

        return new Namespace(parent ?? Identifier.FromEncoded("root"), children.ToAsyncEnumerable());
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }

    public async Task DropNamespaceAsync(
        Identifier identifier,
        bool recursive = false,
        bool purgeData = false,
        CancellationToken cancellationToken = default)
    {
        if (recursive)
        {
            var ns = GetNamespace(identifier, cancellationToken);
            await foreach (var child in ns.Children.WithCancellation(cancellationToken))
                switch (child)
                {
                    case Namespace childNamespace:
                        await DropNamespaceAsync(childNamespace.Identifier, true, purgeData, cancellationToken);
                        break;
                    case Table childTable:
                        // TODO replace with own implementation
                        await ApiClient.DropTableAsync(
                            ns.Identifier.GetEncoded(),
                            childTable.Identifier.GetTableName(),
                            null,
                            purgeData,
                            cancellationToken);
                        break;
                    default:
                        throw new ArgumentException(nameof(child));
                }
        }

        await ApiClient.DropNamespaceAsync(identifier.GetEncoded(), null, cancellationToken);
    }

    public static async Task<RestCatalog> Create(UserConfig userConfig, CancellationToken cancellationToken = default)
    {
        var httpClient = new HttpClient();
        var client = new RestCatalogClient(httpClient)
        {
            BaseUrl = userConfig.BaseUrl
        };

        foreach (var kvp in userConfig.RequestHeaders) httpClient.DefaultRequestHeaders.Add(kvp.Key, kvp.Value);

        var catalogConfig = await client.GetConfigAsync(userConfig.Warehouse, cancellationToken);
        var typedConfig = new TypedCatalogConfig(catalogConfig, userConfig);

        // TODO make this better
        client.BaseUrl = $"{client.BaseUrl}{typedConfig.Prefix}";

        return new RestCatalog(client, userConfig, typedConfig, httpClient);
    }
}