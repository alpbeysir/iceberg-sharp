using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Util;

using Iceberg.Net.Storage;

namespace Iceberg.Net.S3;

public class S3ObjectStorage(S3Config config) : IObjectStorage
{
    public static IReadOnlySet<string> Schemes { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "s3"
    };

    public static IObjectStorage Create(IReadOnlyDictionary<string, string> properties)
    {
        return new S3ObjectStorage(S3Config.FromProperties(properties));
    }

    private readonly AmazonS3Client _client = CreateS3Client(config);

    public async ValueTask<Stream> Open(Uri uri, FileMode fileMode, CancellationToken cancellationToken = default)
    {
        return fileMode switch
        {
            FileMode.CreateNew => await S3SequentialMultipartUploadStream.Create(
                _client,
                new AmazonS3Uri(uri),
                cancellationToken),
            FileMode.Create => throw new InvalidOperationException(),
            FileMode.Open => await S3SeekableReadStream.Create(_client, new AmazonS3Uri(uri), cancellationToken),
            FileMode.OpenOrCreate => throw new InvalidOperationException(),
            FileMode.Truncate => throw new InvalidOperationException(),
            FileMode.Append => throw new InvalidOperationException(),
            _ => throw new ArgumentOutOfRangeException(nameof(fileMode), fileMode, null)
        };
    }

    private static AmazonS3Client CreateS3Client(S3Config config)
    {
        AWSCredentials awsCredentials = config.SessionToken is not null
            ? new SessionAWSCredentials(
                config.AccessKeyId,
                config.SecretAccessKey,
                config.SessionToken)
            : new BasicAWSCredentials(config.AccessKeyId, config.SecretAccessKey);
        AmazonS3Config s3Config = new()
        {
            ServiceURL = config.Endpoint,
            ForcePathStyle = config.ForcePathStyle
        };
        AmazonS3Client s3Client = new(awsCredentials, s3Config);
        return s3Client;
    }
}
