using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Result used when a table is successfully loaded.
///     <br />
///     <br />
///     <br />The table metadata JSON is returned in the `metadata` field. The corresponding file location of table
///     metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For
///     example, a create transaction may return metadata that is staged but not committed.
///     <br />Clients can check whether metadata has changed by comparing metadata locations after the table has been
///     created.
///     <br />
///     <br />
///     <br />The `config` map returns table-specific configuration for the table's resources, including its HTTP client
///     and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its
///     underlying storage.
///     <br />
///     <br />
///     <br />The following configurations should be respected by clients:
///     <br />
///     <br />## General Configurations
///     <br />
///     <br />- `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled
///     <br />
///     <br />## AWS Configurations
///     <br />
///     <br />The following configurations should be respected when working with tables stored in AWS S3
///     <br /> - `client.region`: region to configure client for making requests to AWS
///     <br /> - `s3.access-key-id`: id for credentials that provide access to the data in S3
///     <br /> - `s3.secret-access-key`: secret for credentials that provide access to data in S3
///     <br /> - `s3.session-token`: if present, this value should be used for as the session token
///     <br /> - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the
///     `s3-signer-open-api.yaml` specification
///     <br /> - `s3.cross-region-access-enabled`: if `true`, S3 Cross-Region bucket access is enabled
///     <br />
///     <br />## Storage Credentials
///     <br />
///     <br />Credentials for ADLS / GCS / S3 / ... are provided through the `storage-credentials` field.
///     <br />Clients must first check whether the respective credentials exist in the `storage-credentials` field before
///     checking the `config` for credentials.
///     <br />
/// </summary>
[method: JsonConstructor]
public class LoadTableResult(
    IDictionary<string, string>? config,
    Metadata.TableMetadata metadata,
    string metadataLocation,
    List<StorageCredential>? storageCredentials)
{
    /// <summary>
    ///     May be null if the table is staged as part of a transaction
    /// </summary>
    [JsonPropertyName("metadata-location")]
    public string MetadataLocation { get; } = metadataLocation;

    [JsonPropertyName("metadata")] public Metadata.TableMetadata Metadata { get; } = metadata;

    [JsonPropertyName("config")] public IDictionary<string, string>? Config { get; } = config;

    [JsonPropertyName("storage-credentials")]
    public List<StorageCredential>? StorageCredentials { get; } = storageCredentials;
}
