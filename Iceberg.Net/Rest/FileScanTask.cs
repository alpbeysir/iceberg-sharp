using System.Text.Json.Serialization;
using Iceberg.Net.Rest.Expression;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class FileScanTask(
    DataFile dataFile,
    ICollection<int> deleteFileReferences,
    IExpression residualFilter)
{
    [JsonPropertyName("data-file")] public DataFile DataFile { get; } = dataFile;

    /// <summary>
    ///     A list of indices in the delete files array (0-based)
    /// </summary>
    [JsonPropertyName("delete-file-references")]
    public ICollection<int> DeleteFileReferences { get; } = deleteFileReferences;

    /// <summary>
    ///     An optional filter to be applied to rows in this file scan task.
    ///     <br />If the residual is not present, the client must produce the residual or use the original filter.
    /// </summary>
    [JsonPropertyName("residual-filter")]
    public IExpression ResidualFilter { get; } = residualFilter;
}