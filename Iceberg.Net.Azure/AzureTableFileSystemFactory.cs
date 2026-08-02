using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;
using Iceberg.Net.Storage;

namespace Iceberg.Net.Azure;

public sealed class AzureTableFileSystemFactory : ITableFileSystemFactory
{
    public static IReadOnlySet<string> Schemes { get; } =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "abfs", "abfss", "wasb", "wasbs" };

    public static ITableFileSystem Create(Uri uri, PropertyResolver resolve)
    {
        if (!uri.IsAbsoluteUri || !Schemes.Contains(uri.Scheme))
            throw new ArgumentException("Azure table file paths must use an ABFS or WASB URI", nameof(uri));

        string container = Uri.UnescapeDataString(uri.UserInfo);
        if (string.IsNullOrWhiteSpace(container) || string.IsNullOrWhiteSpace(uri.Host))
            throw new ArgumentException(
                "Azure table file paths must have the form abfss://container@account.dfs.core.windows.net/path",
                nameof(uri));

        string account = uri.Host.Split('.', 2)[0];
        ADLSConfig config = ADLSConfig.FromResolver(account, resolve);
        BlobContainerClient client = CreateContainerClient(uri, container, config);
        return new AzureTableFileSystem(client);
    }

    private static BlobContainerClient CreateContainerClient(
        Uri location,
        string container,
        ADLSConfig config)
    {
        string? connectionString = config.ConnectionString;
        if (!string.IsNullOrWhiteSpace(connectionString) && connectionString.Contains("AccountName=", StringComparison.OrdinalIgnoreCase))
            return new BlobContainerClient(connectionString, container);

        Uri endpoint = CreateContainerEndpoint(location, container, connectionString);

        string? sasToken = config.SasToken;
        if (!string.IsNullOrWhiteSpace(sasToken))
            return new BlobContainerClient(endpoint, new AzureSasCredential(sasToken.TrimStart('?')));

        string? sharedKeyAccount = config.SharedKeyAccountName;
        string? sharedKey = config.SharedKeyAccountKey;
        if (sharedKeyAccount is not null || sharedKey is not null)
        {
            if (string.IsNullOrWhiteSpace(sharedKeyAccount) || string.IsNullOrWhiteSpace(sharedKey))
                throw new InvalidOperationException(
                    "Both 'adls.auth.shared-key.account.name' and 'adls.auth.shared-key.account.key' are required");
            return new BlobContainerClient(endpoint, new StorageSharedKeyCredential(sharedKeyAccount, sharedKey));
        }

        return new BlobContainerClient(endpoint);
    }

    private static Uri CreateContainerEndpoint(Uri location, string container, string? configuredEndpoint)
    {
        if (!string.IsNullOrWhiteSpace(configuredEndpoint))
        {
            if (!Uri.TryCreate(configuredEndpoint, UriKind.Absolute, out Uri? endpoint))
                throw new InvalidOperationException($"Invalid ADLS endpoint '{configuredEndpoint}'");
            return new Uri(endpoint.AbsoluteUri.TrimEnd('/') + "/" + Uri.EscapeDataString(container));
        }

        string host = location.Host.Replace(".dfs.", ".blob.", StringComparison.OrdinalIgnoreCase);
        string scheme = location.Scheme is "abfs" or "wasb" ? Uri.UriSchemeHttp : Uri.UriSchemeHttps;
        return new UriBuilder(scheme, host) { Path = Uri.EscapeDataString(container) }.Uri;
    }
}
