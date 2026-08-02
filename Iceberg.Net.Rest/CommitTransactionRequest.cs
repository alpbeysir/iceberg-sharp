using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class CommitTransactionRequest(ICollection<CommitTableRequest> tableChanges)
{
    [JsonPropertyName("table-changes")] public ICollection<CommitTableRequest> TableChanges { get; } = tableChanges;
}