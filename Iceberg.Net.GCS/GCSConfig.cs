using Iceberg.Net.Storage;

namespace Iceberg.Net.GCS;

public sealed record GCSConfig
{
    public string? AccessToken { get; private init; }

    public bool NoAuth { get; private init; }

    public string? ServiceHost { get; private init; }

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