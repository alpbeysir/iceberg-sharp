using System.Text.Json.Serialization;

namespace Iceberg.Net.Rest;

public enum FileFormat
{
    [JsonStringEnumMemberName("avro")] Avro = 0,

    [JsonStringEnumMemberName("orc")] Orc = 1,

    [JsonStringEnumMemberName("parquet")] Parquet = 2,

    [JsonStringEnumMemberName("puffin")] Puffin = 3
}