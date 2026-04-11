using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CompletedPlanningWithIdResult(
    List<DeleteFile> deleteFiles,
    List<FileScanTask> fileScanTasks,
    string planId,
    List<string> planTasks,
    PlanStatus status,
    List<StorageCredential> storageCredentials)
    : CompletedPlanningResult(deleteFiles, fileScanTasks, planTasks, status, storageCredentials)
{
    /// <summary>
    ///     ID used to track a planning request
    /// </summary>
    [JsonPropertyName("plan-id")]
    public string PlanId { get; } = planId;
}