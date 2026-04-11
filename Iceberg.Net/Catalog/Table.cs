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

    private Uri BaseFolderUri
    {
        get
        {
            var success = Uri.TryCreate(Metadata!.Location + '/', UriKind.RelativeOrAbsolute, out var folder);
            return !success ? throw new FormatException("Location URI was malformed") : folder!;
        }
    }

    public Uri MetadataFolderUri => new(BaseFolderUri, "metadata/");

    public Uri DataFolderUri => new(BaseFolderUri, "data/");

    public Identifier Identifier { get; }
    public ICatalog Catalog { get; }

    public IObjectStorage ObjectStorage => _objectStorageLazy.Value;

    internal void Initialize(TableMetadata? metadata, IReadOnlyList<StorageCredential>? storageCredentials)
    {
        Metadata = metadata ?? Metadata;
        StorageCredentials = storageCredentials ?? StorageCredentials;
    }

    private IObjectStorage GetObjectStorage()
    {
        // TODO switch here for things other than S3
        // TODO handle multiple credentials by prefix
        S3Config s3Config;
        if (StorageCredentials is null)
            s3Config = Catalog.StorageConfig as S3Config ?? throw new InvalidOperationException("Only S3 is supported");
        else
            s3Config = S3Config.FromStorageCredential(StorageCredentials[0]);
        return new S3ObjectStorage(s3Config);
    }
}