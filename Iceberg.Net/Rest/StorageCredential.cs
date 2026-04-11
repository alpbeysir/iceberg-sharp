using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class StorageCredential(IDictionary<string, string> config, string prefix)
{
    /// <summary>
    ///     Indicates a storage location prefix where the credential is relevant. Clients should choose the most specific
    ///     prefix (by selecting the longest prefix) if several credentials of the same type are available.
    /// </summary>
    [JsonPropertyName("prefix")]
    public string Prefix { get; } = prefix;

    [JsonPropertyName("config")] public IDictionary<string, string> Config { get; } = config;
}