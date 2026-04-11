using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Server-provided configuration for the catalog.
/// </summary>
[method: JsonConstructor]
public class CatalogConfig(
    IDictionary<string, string> defaults,
    ICollection<string> endpoints,
    TimeSpan? idempotencyKeyLifetime,
    IDictionary<string, string> overrides)
{
    /// <summary>
    ///     Properties that should be used to override client configuration; applied after defaults and client configuration.
    /// </summary>
    [JsonPropertyName("overrides")]
    public IDictionary<string, string> Overrides { get; } = overrides;

    /// <summary>
    ///     Properties that should be used as default configuration; applied before client configuration.
    /// </summary>
    [JsonPropertyName("defaults")]
    public IDictionary<string, string> Defaults { get; } = defaults;

    /// <summary>
    ///     A list of endpoints that the server supports. The format of each endpoint must be "&lt;HTTP verb&gt; &lt;resource
    ///     path from OpenAPI REST spec&gt;". The HTTP verb and the resource path must be separated by a space character.
    /// </summary>
    [JsonPropertyName("endpoints")]
    public ICollection<string> Endpoints { get; } = endpoints;

    /// <summary>
    ///     Client reuse window for an Idempotency-Key (ISO-8601 duration, e.g., PT30M, PT24H). Interpreted as the maximum time
    ///     from the first submission using a key to the last retry during which a client may reuse that key. Servers SHOULD
    ///     accept retries for at least this duration and MAY include a grace period to account for delays/clock skew. Clients
    ///     SHOULD NOT reuse an Idempotency-Key after this window elapses; they SHOULD generate a new key for any subsequent
    ///     attempt. Presence of this field indicates the server supports Idempotency-Key semantics for mutation endpoints. If
    ///     absent, clients MUST assume idempotency is not supported.
    /// </summary>
    [JsonPropertyName("idempotency-key-lifetime")]
    public TimeSpan? IdempotencyKeyLifetime { get; } = idempotencyKeyLifetime;
}