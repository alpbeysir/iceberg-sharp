using Iceberg.Net.Storage;

namespace Iceberg.Net.GCS;

public sealed record GCSConfig
{
    public string? AccessToken { get; init; }

    public bool NoAuth { get; init; }

    public string? ServiceHost { get; init; }

    public static GCSConfig FromResolver(PropertyResolver resolve)
    {
        string? noAuth = resolve("gcs.no-auth");
        return new GCSConfig
        {
            AccessToken = resolve("gcs.oauth2.token"),
            NoAuth = bool.TryParse(noAuth, out bool configuredNoAuth) && configuredNoAuth,
            ServiceHost = resolve("gcs.service.host")
        };
    }
}
