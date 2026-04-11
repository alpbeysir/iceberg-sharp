using System.Text.Json.Serialization;
using Iceberg.Net.Rest.TableRequirement;
using Iceberg.Net.Rest.TableUpdate;

namespace Iceberg.Net.Rest;

public record CommitTableRequest
{
    /// <summary>
    ///     Table identifier to update; must be present for CommitTransactionRequest
    /// </summary>
    [JsonPropertyName("identifier")]
    public required TableIdentifier Identifier { get; init; }

    [JsonPropertyName("requirements")] public required List<ITableRequirement> Requirements { get; init; }

    [JsonPropertyName("updates")] public required List<ITableUpdate> Updates { get; init; }
}