using System.Runtime.Serialization;

namespace Iceberg.Net.Metadata;

public enum SortDirection
{
    [EnumMember(Value = @"asc")] Asc = 0,

    [EnumMember(Value = @"desc")] Desc = 1
}