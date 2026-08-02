using Iceberg.Net.Storage;

namespace Iceberg.Net.S3;

public record S3Config
{
    public required string AccessKeyId;
    public required string Endpoint;
    public bool ForcePathStyle;
    public required string SecretAccessKey;
    public string? SessionToken;

    public static S3Config FromResolver(PropertyResolver resolve)
    {
        string? sessionToken = resolve("s3.session-token");
        string? pathStyleAccess = resolve("s3.path-style-access");
        return new S3Config
        {
            Endpoint = ResolveEndpoint(resolve),
            AccessKeyId = resolve("s3.access-key-id")
                ?? throw new InvalidOperationException("Missing required object storage property 's3.access-key-id'"),
            SecretAccessKey = resolve("s3.secret-access-key")
                ?? throw new InvalidOperationException("Missing required object storage property 's3.secret-access-key'"),
            SessionToken = sessionToken,
            ForcePathStyle = bool.TryParse(pathStyleAccess, out bool forcePathStyle) && forcePathStyle
        };
    }

    public IReadOnlyDictionary<string, string> ToProperties()
    {
        var properties = new Dictionary<string, string>
        {
            ["s3.endpoint"] = Endpoint,
            ["s3.access-key-id"] = AccessKeyId,
            ["s3.secret-access-key"] = SecretAccessKey,
            ["s3.path-style-access"] = ForcePathStyle.ToString()
        };
        if (SessionToken is not null) properties["s3.session-token"] = SessionToken;
        return properties;
    }

    private static string ResolveEndpoint(PropertyResolver resolve)
    {
        string? customEndpoint = resolve("s3.endpoint");
        if (customEndpoint is not null) return customEndpoint;

        string? customRegion = resolve("s3.region");
        var region = customRegion ?? "us-east-1";
        return $"https://s3.{region}.amazonaws.com/";
    }
}
