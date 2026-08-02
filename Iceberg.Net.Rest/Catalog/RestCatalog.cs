using System.Net;
using System.Runtime.CompilerServices;
using Iceberg.Net.Rest;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;
using Iceberg.Net.Schemas;
using Iceberg.Net.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Iceberg.Net.Catalog;

public record UserConfig
{
    public readonly Dictionary<string, string> CatalogConfig = new();
    public readonly Dictionary<string, string> RequestHeaders = new();
    public required string BaseUrl;
    public ILoggerFactory LoggerFactory { get; init; } = NullLoggerFactory.Instance;
    public string? Warehouse;
}

public record TypedCatalogConfig(CatalogConfig CatalogConfig, UserConfig UserConfig)
{
    public string? Prefix => Resolve("prefix");
    public string NamespaceSeparator => Uri.UnescapeDataString(Resolve("namespace-separator") ?? "%1F");

    public string? Resolve(string key)
    {
        CatalogConfig.Defaults.TryGetValue(key, out string? catalogDefault);
        UserConfig.CatalogConfig.TryGetValue(key, out string? userOverride);
        CatalogConfig.Overrides.TryGetValue(key, out string? catalogOverride);
        return catalogOverride ?? userOverride ?? catalogDefault;
    }
}

public sealed class RestCatalog : ICatalog
{
    private const int PageSize = 10;
    private readonly HttpClient _httpClient;
    private readonly ILogger<RestCatalog> _logger;

    private RestCatalog(
        RestCatalogClient apiClient,
        UserConfig userConfig,
        TypedCatalogConfig catalogConfig,
        HttpClient httpClient)
    {
        _httpClient = httpClient;
        LoggerFactory = userConfig.LoggerFactory;
        _logger = LoggerFactory.CreateLogger<RestCatalog>();
        ApiClient = apiClient;
        UserConfig = userConfig;
        CatalogConfig = catalogConfig;
    }

    private RestCatalogClient ApiClient { get; }
    private UserConfig UserConfig { get; }
    public ILoggerFactory LoggerFactory { get; }
    public TypedCatalogConfig CatalogConfig { get; }

    public string? Resolve(string key)
    {
        return CatalogConfig.Resolve(key);
    }

    private string EncodeNamespace(Identifier identifier)
    {
        return identifier.GetEncoded(CatalogConfig.NamespaceSeparator);
    }

    public async Task<Table> CreateTableAsync(
        Identifier identifier,
        Schema schema,
        bool stage,
        CancellationToken cancellationToken)
    {
        return await CreateTableAsync(
            identifier,
            schema,
            null,
            stage,
            cancellationToken);
    }

    public async Task<Table> CreateTableAsync(
        Identifier identifier,
        Schema schema,
        IReadOnlyDictionary<string, string>? properties,
        bool stage,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Creating table {TableIdentifier} (staged: {Stage})",
            identifier.ToString(),
            stage);
        CreateTableRequest request = new()
        {
            Name = identifier.GetTableName(),
            Schema = schema,
            Location = null,
            PartitionSpec = null,
            WriteOrder = null,
            StageCreate = stage,
            Properties = properties?.ToDictionary(
                property => property.Key,
                property => property.Value,
                StringComparer.Ordinal)
        };
        LoadTableResult response = await ApiClient.CreateTableAsync(
            request,
            EncodeNamespace(identifier.GetParent()),
            cancellationToken: cancellationToken);
        _logger.LogInformation("Created table {TableIdentifier}", identifier.ToString());
        return new Table(identifier, this, response.Metadata)
        {
            PropertyResolver = CreatePropertyResolver(response.Config),
            StorageCredentials = response.StorageCredentials?.ToArray() ?? []
        };
    }

    public async Task<Table> UpdateTableAsync(
        Table table,
        List<ITableUpdate> updates,
        List<ITableRequirement> requirements,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Committing {UpdateCount} updates to table {TableIdentifier} with {RequirementCount} requirements",
            updates.Count,
            table.Identifier.ToString(),
            requirements.Count);
        CommitTableResponse response = await ApiClient.UpdateTableAsync(
            new CommitTableRequest
            {
                Identifier = table.Identifier.ToTableIdentifier(),
                Requirements = requirements,
                Updates = updates
            },
            EncodeNamespace(table.Identifier.GetParent()),
            table.Identifier.GetTableName(),
            cancellationToken: cancellationToken);
        _logger.LogInformation(
            "Committed table {TableIdentifier} at snapshot {SnapshotId}",
            table.Identifier.ToString(),
            response.Metadata.CurrentSnapshotId);
        return table with
        {
            Metadata = response.Metadata,
            PropertyResolver = response.Config is null
                ? table.PropertyResolver
                : CreatePropertyResolver(response.Config)
        };
    }

    public async Task<Table?> LoadTableAsync(
        Identifier identifier,
        Snapshots snapshots = Snapshots.All,
        CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Loading table {TableIdentifier}", identifier.ToString());
        try
        {
            LoadTableResult response = await ApiClient.LoadTableAsync(
                EncodeNamespace(identifier.GetParent()),
                identifier.GetTableName(),
                snapshots: snapshots,
                cancellationToken: cancellationToken);
            _logger.LogDebug(
                "Loaded table {TableIdentifier} at snapshot {SnapshotId}",
                identifier.ToString(),
                response.Metadata.CurrentSnapshotId);
            return new Table(identifier, this, response.Metadata)
            {
                PropertyResolver = CreatePropertyResolver(response.Config),
                StorageCredentials = response.StorageCredentials?.ToArray() ?? []
            };
        }
        catch (IcebergRestException exception) when (exception.StatusCode == (int)HttpStatusCode.NotFound)
        {
            _logger.LogDebug("Table {TableIdentifier} was not found", identifier.ToString());
            return null;
        }
    }

    public Namespace GetNamespace(Identifier identifier, CancellationToken cancellationToken = default)
    {
        IAsyncEnumerable<INode> childNamespaces = ListNamespacesAsync(identifier, cancellationToken);
        IAsyncEnumerable<INode> childTables = ListTablesAsync(identifier, cancellationToken);
        return new Namespace(identifier, childNamespaces.Concat(childTables));
    }

    private static PropertyResolver? CreatePropertyResolver(
        IEnumerable<KeyValuePair<string, string>>? properties)
    {
        if (properties is null) return null;
        IReadOnlyDictionary<string, string> values =
            properties.ToDictionary(property => property.Key, property => property.Value, StringComparer.Ordinal);
        return key => values.TryGetValue(key, out string? value) ? value : null;
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
        foreach (string part in identifier) ns.Add(part);
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
            foreach (TableIdentifier tableIdentifier in resp.Identifiers)
            {
                Table? table = await LoadTableAsync(
                    tableIdentifier.ToIdentifier(),
                    cancellationToken: cancellationToken);
                if (table is not null) yield return table;
            }

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
            List<INode> children = [];
            await foreach (INode child in ns.Children.WithCancellation(cancellationToken))
                children.Add(child);

            foreach (INode child in children)
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
        ILogger<RestCatalog> logger = userConfig.LoggerFactory.CreateLogger<RestCatalog>();
        logger.LogInformation("Connecting to REST catalog at {BaseUrl}", userConfig.BaseUrl);
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

        logger.LogInformation("REST catalog configured at {BaseUrl}", client.BaseUrl);

        return new RestCatalog(client, userConfig, typedConfig, httpClient);
    }
}
