using Amazon.Runtime;
using Amazon.S3;
using EngineeredWood.IO;
using EngineeredWood.IO.Aws;
using Iceberg.Net.Storage;

namespace Iceberg.Net.S3;

public sealed class S3TableFileSystemFactory : ITableFileSystemFactory
{
    public static IReadOnlySet<string> Schemes { get; } =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "s3" };

    public static ITableFileSystem Create(Uri uri, PropertyResolver resolve)
    {
        if (!uri.IsAbsoluteUri || !uri.Scheme.Equals("s3", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("S3 table file paths must be absolute s3:// URIs", nameof(uri));
        if (string.IsNullOrWhiteSpace(uri.Host))
            throw new ArgumentException("S3 table file paths must include a bucket", nameof(uri));

        S3Config config = S3Config.FromResolver(resolve);
        return new S3TableFileSystem(CreateS3Client(config), uri.Host);
    }

    private static IAmazonS3 CreateS3Client(S3Config config)
    {
        AWSCredentials credentials = config.SessionToken is not null
            ? new SessionAWSCredentials(
                config.AccessKeyId,
                config.SecretAccessKey,
                config.SessionToken)
            : new BasicAWSCredentials(config.AccessKeyId, config.SecretAccessKey);
        AmazonS3Config clientConfig = new()
        {
            ServiceURL = config.Endpoint,
            ForcePathStyle = config.ForcePathStyle
        };
        return new AmazonS3Client(credentials, clientConfig);
    }
}
