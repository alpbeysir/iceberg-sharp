using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Scan and planning tasks for server-side scan planning
///     <br />
///     <br />- `plan-tasks` contains opaque units of planning work
///     <br />- `file-scan-tasks` contains a partial or complete list of table scan tasks
///     <br />- `delete-files` contains delete files referenced by file scan tasks
///     <br />
///     <br />Each plan task must be passed to the fetchScanTasks endpoint to fetch the file scan tasks for the plan task.
///     <br />
///     <br />The list of delete files must contain all delete files referenced by the file scan tasks.
///     <br />
/// </summary>
[method: JsonConstructor]
public class ScanTasks(
    List<DeleteFile> deleteFiles,
    List<FileScanTask> fileScanTasks,
    List<string> planTasks)
{
    /// <summary>
    ///     Delete files referenced by file scan tasks
    /// </summary>
    [JsonPropertyName("delete-files")]
    public List<DeleteFile> DeleteFiles { get; } = deleteFiles;

    [JsonPropertyName("file-scan-tasks")] public List<FileScanTask> FileScanTasks { get; } = fileScanTasks;

    [JsonPropertyName("plan-tasks")] public List<string> PlanTasks { get; } = planTasks;
}