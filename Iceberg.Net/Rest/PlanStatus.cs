using System.Runtime.Serialization;

namespace Iceberg.Net.Rest;

/// <summary>
///     Status of a server-side planning operation
/// </summary>
public enum PlanStatus
{
    [EnumMember(Value = @"completed")] Completed = 0,

    [EnumMember(Value = @"submitted")] Submitted = 1,

    [EnumMember(Value = @"cancelled")] Cancelled = 2,

    [EnumMember(Value = @"failed")] Failed = 3
}