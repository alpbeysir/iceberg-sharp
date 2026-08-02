using Iceberg.Net.Metadata;
using Iceberg.Net.Rest;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Catalog;

public sealed record Table(Identifier Identifier, ICatalog Catalog) : INode
{
    public bool IsLoaded => Metadata is not null;

    public TableMetadata? Metadata { get; init; }

    public PropertyResolver? PropertyResolver { get; init; }

    public IReadOnlyList<StorageCredential> StorageCredentials { get; init; } = [];

    public TablePropertyResolver Properties => new(Metadata?.Properties);

    private Uri BaseFolderUri
    {
        get
        {
            bool success = Uri.TryCreate(Metadata!.Location + '/', UriKind.RelativeOrAbsolute, out Uri? folder);
            return !success ? throw new FormatException("Location URI was malformed") : folder!;
        }
    }

    public Uri MetadataFolderUri => ResolveFolderUri(
        TableProperties.WriteMetadataLocation,
        "metadata/");

    public Uri DataFolderUri => ResolveFolderUri(
        TableProperties.WriteDataLocation,
        "data/");

    internal ValueTask<Stream> Open(
        Uri uri,
        FileMode fileMode = FileMode.Open,
        CancellationToken cancellationToken = default)
    {
        return ObjectStorageRegistry.Resolve(uri, Resolve, StorageCredentials)
            .Open(uri, fileMode, cancellationToken);
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
}