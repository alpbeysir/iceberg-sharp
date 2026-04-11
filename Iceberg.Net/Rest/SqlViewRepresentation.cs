using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

[method: JsonConstructor]
public class SqlViewRepresentation(string dialect, string sql, string type)
{
    [JsonPropertyName("type")] public string Type { get; } = type;

    [JsonPropertyName("sql")] public string Sql { get; } = sql;

    [JsonPropertyName("dialect")] public string Dialect { get; } = dialect;
}