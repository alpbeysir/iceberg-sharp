namespace Iceberg.Net.S3;

public record S3Config
{
    public required string AccessKeyId;
    public required string Endpoint;
    public bool ForcePathStyle;
    public required string SecretAccessKey;
    public string? SessionToken;

    public static S3Config FromProperties(IReadOnlyDictionary<string, string> properties)
    {
        properties.TryGetValue("s3.session-token", out var sessionToken);
        properties.TryGetValue("s3.path-style-access", out var pathStyleAccess);
        return new S3Config
        {
            Endpoint = ResolveEndpoint(properties),
            AccessKeyId = properties["s3.access-key-id"],
            SecretAccessKey = properties["s3.secret-access-key"],
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

    private static string ResolveEndpoint(IReadOnlyDictionary<string, string> properties)
    {
        bool hasCustomEndpoint = properties.TryGetValue("s3.endpoint", out var customEndpoint);
        if (hasCustomEndpoint) return customEndpoint!;

        properties.TryGetValue("s3.region", out var customRegion);
        var region = customRegion ?? "us-east-1";
        return $"https://s3.{region}.amazonaws.com/";
    }
}
