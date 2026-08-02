using Iceberg.Net.Metadata;
using Iceberg.Net.Rest;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Catalog;

public sealed record Table : INode
{
    public Table(Identifier identifier, ICatalog catalog)
    {
        Identifier = identifier;
        Catalog = catalog;
    }

    public bool IsLoaded => Metadata is not null;

    public TableMetadata? Metadata { get; private set; }

    private IReadOnlyList<StorageCredential>? StorageCredentials { get; set; }
    private IReadOnlyDictionary<string, string>? ObjectStorageProperties { get; set; }

    private Uri BaseFolderUri
    {
        get
        {
            bool success = Uri.TryCreate(Metadata!.Location + '/', UriKind.RelativeOrAbsolute, out Uri? folder);
            return !success ? throw new FormatException("Location URI was malformed") : folder!;
        }
    }

    public Uri MetadataFolderUri => new(BaseFolderUri, "metadata/");

    public Uri DataFolderUri => new(BaseFolderUri, "data/");

    public Identifier Identifier { get; }
    public ICatalog Catalog { get; }

    internal void Initialize(
        TableMetadata? metadata,
        IReadOnlyList<StorageCredential>? storageCredentials,
        IEnumerable<KeyValuePair<string, string>>? objectStorageProperties = null)
    {
        Metadata = metadata ?? Metadata;
        StorageCredentials = storageCredentials ?? StorageCredentials;
        ObjectStorageProperties = objectStorageProperties?.ToDictionary() ?? ObjectStorageProperties;
    }

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
        return ObjectStorageProperties is not null && ObjectStorageProperties.TryGetValue(key, out string? value)
            ? value
            : Catalog.Resolve(key);
    }
}
