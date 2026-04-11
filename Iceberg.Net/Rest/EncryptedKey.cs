using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class EncryptedKey(
    string encryptedById,
    byte[] encryptedKeyMetadata,
    string keyId,
    IDictionary<string, string> properties)
{
    [JsonPropertyName("key-id")] public string KeyId { get; } = keyId;

    [JsonPropertyName("encrypted-key-metadata")]
    public byte[] EncryptedKeyMetadata { get; } = encryptedKeyMetadata;

    [JsonPropertyName("encrypted-by-id")] public string EncryptedById { get; } = encryptedById;

    [JsonPropertyName("properties")] public IDictionary<string, string> Properties { get; } = properties;
}