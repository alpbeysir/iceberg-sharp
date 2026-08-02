using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Response schema for fetchScanTasks
/// </summary>
[method: JsonConstructor]
public class FetchScanTasksResult(
    List<DeleteFile> deleteFiles,
    List<FileScanTask> fileScanTasks,
    List<string> planTasks)
    : ScanTasks(deleteFiles, fileScanTasks, planTasks);