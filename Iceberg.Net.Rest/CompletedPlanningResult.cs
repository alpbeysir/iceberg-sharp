using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Completed server-side planning result
/// </summary>
[method: JsonConstructor]
public class CompletedPlanningResult(
    List<DeleteFile> deleteFiles,
    List<FileScanTask> fileScanTasks,
    List<string> planTasks,
    PlanStatus status,
    List<StorageCredential> storageCredentials)
    : ScanTasks(deleteFiles, fileScanTasks, planTasks)
{
    [JsonPropertyName("status")]
    [JsonConverter(typeof(JsonStringEnumConverter<PlanStatus>))]
    public PlanStatus Status { get; } = status;

    /// <summary>
    ///     Storage credentials for accessing the files returned in the scan result.
    ///     <br />If the server returns storage credentials as part of the completed scan planning response, the expectation is
    ///     for the client to use these credentials to read the files returned in the FileScanTasks as part of the scan result.
    /// </summary>
    [JsonPropertyName("storage-credentials")]
    public List<StorageCredential> StorageCredentials { get; } = storageCredentials;
}