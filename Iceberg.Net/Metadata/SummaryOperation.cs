using System.Text.Json.Serialization;

namespace Iceberg.Net.Metadata;

public enum SummaryOperation
{
    [JsonStringEnumMemberName("append")] Append = 0,
    [JsonStringEnumMemberName("replace")] Replace = 1,

    [JsonStringEnumMemberName("overwrite")]
    Overwrite = 2,
    [JsonStringEnumMemberName("delete")] Delete = 3
}