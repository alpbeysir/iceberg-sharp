using EngineeredWood.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.Rest;
using Iceberg.Net.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Iceberg.Net.Catalog;

public sealed record Table(
    Identifier Identifier,
    ICatalog Catalog,
    TableMetadata Metadata) : INode
{
    private readonly ILogger<Table> _logger =
        Catalog?.LoggerFactory.CreateLogger<Table>() ?? NullLogger<Table>.Instance;

    public PropertyResolver? PropertyResolver { get; init; }

    public IReadOnlyList<StorageCredential> StorageCredentials { get; init; } = [];

    public TablePropertyResolver Properties => new(Metadata.Properties);

    public TableOperations Operations()
    {
        return new TableOperations(this);
    }

    private Uri BaseFolderUri
    {
        get
        {
            bool success = Uri.TryCreate(Metadata.Location + '/', UriKind.RelativeOrAbsolute, out Uri? folder);
            return !success ? throw new FormatException("Location URI was malformed") : folder!;
        }
    }

    public Uri MetadataFolderUri => ResolveFolderUri(
        TableProperties.WriteMetadataLocation,
        "metadata/");

    public Uri DataFolderUri => ResolveFolderUri(
        TableProperties.WriteDataLocation,
        "data/");

    internal async ValueTask<IRandomAccessFile> ReadFile(
        Uri uri,
        CancellationToken cancellationToken = default)
    {
        ITableFileSystem fileSystem = TableFileSystemRegistry.Resolve(uri, Resolve, StorageCredentials);
        IRandomAccessFile file = await fileSystem.OpenReadAsync(GetFileSystemPath(uri), cancellationToken);
        _logger.LogDebug("Opened object storage file {FileUri} for reading", uri);
        return file;
    }

    internal async ValueTask<ISequentialFile> CreateFile(
        Uri uri,
        bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        ITableFileSystem fileSystem = TableFileSystemRegistry.Resolve(uri, Resolve, StorageCredentials);
        ISequentialFile file = await fileSystem.CreateAsync(
            GetFileSystemPath(uri),
            overwrite,
            cancellationToken);
        _logger.LogDebug("Created object storage file {FileUri} for writing", uri);
        return file;
    }

    private string? Resolve(string key)
    {
        return PropertyResolver?.Invoke(key) ?? Catalog.Resolve(key);
    }

    private Uri ResolveFolderUri(string property, string defaultFolder)
    {
        string? configuredLocation = Properties.GetString(property);
        if (configuredLocation is null) return new Uri(BaseFolderUri, defaultFolder);

        if (!Uri.TryCreate(configuredLocation, UriKind.Absolute, out Uri? location))
            throw new FormatException(
                $"Table property '{property}' must be an absolute URI, but was '{configuredLocation}'.");

        return location.AbsoluteUri.EndsWith('/')
            ? location
            : new Uri(location.AbsoluteUri + '/', UriKind.Absolute);
    }

    private static string GetFileSystemPath(Uri uri)
    {
        return uri.GetComponents(UriComponents.Path, UriFormat.Unescaped).TrimStart('/');
    }
}
