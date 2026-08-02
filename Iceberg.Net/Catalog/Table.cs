using Iceberg.Net.Metadata;
using Iceberg.Net.Rest;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Catalog;

public sealed record Table : INode
{
    private readonly Lazy<IObjectStorage> _objectStorageLazy;

    public Table(Identifier identifier, ICatalog catalog)
    {
        Identifier = identifier;
        Catalog = catalog;
        _objectStorageLazy = new Lazy<IObjectStorage>(GetObjectStorage);
    }

    public bool IsLoaded => Metadata is not null;

    public TableMetadata? Metadata { get; private set; }

    private IReadOnlyList<StorageCredential>? StorageCredentials { get; set; }
    private IReadOnlyDictionary<string, string>? ObjectStorageProperties { get; set; }

    private Uri BaseFolderUri
    {
        get
        {
            var success = Uri.TryCreate(Metadata!.Location + '/', UriKind.RelativeOrAbsolute, out Uri? folder);
            return !success ? throw new FormatException("Location URI was malformed") : folder!;
        }
    }

    public Uri MetadataFolderUri => new(BaseFolderUri, "metadata/");

    public Uri DataFolderUri => new(BaseFolderUri, "data/");

    public Identifier Identifier { get; }
    public ICatalog Catalog { get; }

    public IObjectStorage ObjectStorage => _objectStorageLazy.Value;

    internal void Initialize(
        TableMetadata? metadata,
        IReadOnlyList<StorageCredential>? storageCredentials,
        IEnumerable<KeyValuePair<string, string>>? objectStorageProperties = null)
    {
        Metadata = metadata ?? Metadata;
        StorageCredentials = storageCredentials ?? StorageCredentials;
        ObjectStorageProperties = objectStorageProperties?.ToDictionary() ?? ObjectStorageProperties;
    }

    private IObjectStorage GetObjectStorage()
    {
        var properties = new Dictionary<string, string>(Catalog.ObjectStorageProperties, StringComparer.Ordinal);
        if (ObjectStorageProperties is not null)
            foreach (KeyValuePair<string, string> property in ObjectStorageProperties)
                properties[property.Key] = property.Value;
        return ObjectStorageRegistry.CreateRouter(properties, StorageCredentials);
    }
}
