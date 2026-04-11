using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Util;

namespace Iceberg.Net.Storage;

public class S3ObjectStorage(S3Config config) : IObjectStorage
{
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
        var s3Config = new AmazonS3Config
        {
            ServiceURL = config.Endpoint,
            ForcePathStyle = config.ForcePathStyle
        };
        var s3Client = new AmazonS3Client(awsCredentials, s3Config);
        return s3Client;
    }
}