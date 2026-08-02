using EngineeredWood.IO;
using EngineeredWood.IO.Gcs;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Iceberg.Net.Storage;

namespace Iceberg.Net.GCS;

public sealed class GcsTableFileSystemFactory : ITableFileSystemFactory
{
    public static IReadOnlySet<string> Schemes { get; } =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "gs", "gcs" };

    public static ITableFileSystem Create(Uri uri, PropertyResolver resolve)
    {
        if (!uri.IsAbsoluteUri || !Schemes.Contains(uri.Scheme))
            throw new ArgumentException("GCS table file paths must use a gs:// or gcs:// URI", nameof(uri));
        if (string.IsNullOrWhiteSpace(uri.Host))
            throw new ArgumentException("GCS table file paths must include a bucket", nameof(uri));

        GCSConfig config = GCSConfig.FromResolver(resolve);
        return new GcsTableFileSystem(CreateStorageClient(config), uri.Host);
    }

    private static StorageClient CreateStorageClient(GCSConfig config)
    {
        string? accessToken = config.AccessToken;
        if (!string.IsNullOrWhiteSpace(accessToken))
            return StorageClient.Create(GoogleCredential.FromAccessToken(accessToken));

        if (config is { NoAuth: false, ServiceHost: null }) return StorageClient.Create();

        StorageClientBuilder builder = new() { UnauthenticatedAccess = config.NoAuth };
        if (config.ServiceHost is not null) builder.BaseUri = config.ServiceHost;
        return builder.Build();
    }
}
