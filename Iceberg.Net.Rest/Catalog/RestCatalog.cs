using System.Net;
using System.Runtime.CompilerServices;
using Iceberg.Net.Rest;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Schemas;

namespace Iceberg.Net.Catalog;

public record UserConfig
{
    public readonly Dictionary<string, string> CatalogConfig = new();
    public readonly Dictionary<string, string> RequestHeaders = new();
    public required string BaseUrl;
    public string? Warehouse;
}

public record TypedCatalogConfig(CatalogConfig CatalogConfig, UserConfig UserConfig)
{
    public string? Prefix => Resolve("prefix");
    public string NamespaceSeparator => Uri.UnescapeDataString(Resolve("namespace-separator") ?? "%1F");

    public string? Resolve(string key)
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

    public string? Resolve(string key)
    {
        return CatalogConfig.Resolve(key);
    }

    private string EncodeNamespace(Identifier identifier)
    {
        return identifier.GetEncoded(CatalogConfig.NamespaceSeparator);
    }

    async Task<Table> ICatalog.CreateTableInternalAsync(
        Identifier identifier,
        Schema schema,
        bool stage,
        CancellationToken cancellationToken)
    {
        CreateTableRequest request = new()
        {
            Name = identifier.GetTableName(),
            Schema = schema,
            Location = null,
            PartitionSpec = null,
            WriteOrder = null,
            StageCreate = stage,
            Properties = null
        };
        LoadTableResult response = await ApiClient.CreateTableAsync(
            request,
            EncodeNamespace(identifier.GetParent()),
            cancellationToken: cancellationToken);
        Table table = new(identifier, this);
        table.Initialize(response.Metadata, response.StorageCredentials, response.Config);
        return table;
    }

    public async Task<Table> UpdateTableAsync(
        Identifier identifier,
        List<ITableUpdate> updates,
        List<ITableRequirement> requirements,
        CancellationToken cancellationToken)
    {
        CommitTableResponse response = await ApiClient.UpdateTableAsync(
            new CommitTableRequest
            {
                Identifier = identifier.ToTableIdentifier(),
                Requirements = requirements,
                Updates = updates
            },
            EncodeNamespace(identifier.GetParent()),
            identifier.GetTableName(),
            cancellationToken: cancellationToken);
        Table table = new(identifier, this);
        table.Initialize(response.Metadata, null, response.Config);
        return table;
    }

    public async Task<Table> LoadTableAsync(
        Identifier identifier,
        Snapshots snapshots = Snapshots.All,
        CancellationToken cancellationToken = default)
    {
        LoadTableResult response = await ApiClient.LoadTableAsync(
            EncodeNamespace(identifier.GetParent()),
            identifier.GetTableName(),
            snapshots: snapshots,
            cancellationToken: cancellationToken);
        Table table = new(identifier, this);
        table.Initialize(response.Metadata, response.StorageCredentials, response.Config);
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
        Rest.Namespace ns = new();
        ns.AddRange(identifier);
        CreateNamespaceRequest request = new(ns, properties ?? new Dictionary<string, string>());
        await ApiClient.CreateNamespaceAsync(request, null, cancellationToken);
    }

    public async Task<bool> NamespaceExistsAsync(Identifier identifier, CancellationToken cancellationToken = default)
    {
        try
        {
            await ApiClient.NamespaceExistsAsync(EncodeNamespace(identifier), cancellationToken);
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
        Rest.Namespace ns = new();
        foreach (var part in identifier) ns.Add(part);
        CreateNamespaceRequest request = new(ns, properties ?? new Dictionary<string, string>());
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
            ListNamespacesResponse resp =
                await ApiClient.ListNamespacesAsync(
                    pageToken,
                    PageSize,
                    parent is { } parentIdentifier ? EncodeNamespace(parentIdentifier) : null,
                    cancellationToken);
            foreach (Rest.Namespace ns in resp.Namespaces)
            {
                Identifier identifier = new(ns);
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
            ListTablesResponse resp =
                await ApiClient.ListTablesAsync(EncodeNamespace(ns), pageToken, PageSize, cancellationToken);
            foreach (TableIdentifier table in resp.Identifiers)
                yield return new Table(table.ToIdentifier(), this);

            pageToken = resp.NextPageToken;
        } while (pageToken != null);
    }

    public async Task<INode> GenerateTree(Identifier? parent = null, CancellationToken cancellationToken = default)
    {
        List<INode> children = [];
        ListNamespacesResponse nsResponse = await ApiClient.ListNamespacesAsync(
            null,
            null,
            parent is { } parentIdentifier ? EncodeNamespace(parentIdentifier) : null,
            cancellationToken);
        foreach (Rest.Namespace ns in nsResponse.Namespaces)
        {
            INode child = await GenerateTree(new Identifier(ns), cancellationToken);
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
            Namespace ns = GetNamespace(identifier, cancellationToken);
            await foreach (INode child in ns.Children.WithCancellation(cancellationToken))
                switch (child)
                {
                    case Namespace childNamespace:
                        await DropNamespaceAsync(childNamespace.Identifier, true, purgeData, cancellationToken);
                        break;
                    case Table childTable:
                        // TODO replace with own implementation
                        await ApiClient.DropTableAsync(
                            EncodeNamespace(ns.Identifier),
                            childTable.Identifier.GetTableName(),
                            null,
                            purgeData,
                            cancellationToken);
                        break;
                    default:
                        throw new ArgumentException(nameof(child));
                }
        }

        await ApiClient.DropNamespaceAsync(EncodeNamespace(identifier), null, cancellationToken);
    }

    public static async Task<ICatalog> Create(UserConfig userConfig, CancellationToken cancellationToken = default)
    {
        HttpClient httpClient = new();
        RestCatalogClient client = new(httpClient)
        {
            BaseUrl = userConfig.BaseUrl
        };

        foreach (KeyValuePair<string, string> kvp in userConfig.RequestHeaders)
            httpClient.DefaultRequestHeaders.Add(kvp.Key, kvp.Value);

        CatalogConfig catalogConfig = await client.GetConfigAsync(userConfig.Warehouse, cancellationToken);
        TypedCatalogConfig typedConfig = new(catalogConfig, userConfig);

        // TODO make this better
        client.BaseUrl = $"{client.BaseUrl}{typedConfig.Prefix}";

        return new RestCatalog(client, userConfig, typedConfig, httpClient);
    }
}
