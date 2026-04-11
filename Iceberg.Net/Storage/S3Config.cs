using Iceberg.Net.Rest;

namespace Iceberg.Net.Storage;

public record S3Config : IStorageConfig
{
    public required string AccessKeyId;
    public required string Endpoint;
    public bool ForcePathStyle;
    public required string SecretAccessKey;
    public string? SessionToken;

    public static S3Config FromStorageCredential(StorageCredential storageCredential)
    {
        storageCredential.Config.TryGetValue("s3.session-token", out var sessionToken);
        return new S3Config
        {
            Endpoint = ResolveEndpoint(storageCredential),
            AccessKeyId = storageCredential.Config["s3.access-key-id"],
            SecretAccessKey = storageCredential.Config["s3.secret-access-key"],
            SessionToken = sessionToken
        };
    }

    private static string ResolveEndpoint(StorageCredential storageCredential)
    {
        bool hasCustomEndpoint = storageCredential.Config.TryGetValue("s3.endpoint", out var customEndpoint);
        if (hasCustomEndpoint) return customEndpoint!;

        storageCredential.Config.TryGetValue("s3.region", out var customRegion);
        var region = customRegion ?? "us-east-1";
        return $"https://s3.{region}.amazonaws.com/";
    }
}

public interface IStorageConfig;