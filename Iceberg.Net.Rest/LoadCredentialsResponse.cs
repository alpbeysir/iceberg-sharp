using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class LoadCredentialsResponse(ICollection<StorageCredential> storageCredentials)
{
    [JsonPropertyName("storage-credentials")]
    public ICollection<StorageCredential> StorageCredentials { get; } = storageCredentials;
}