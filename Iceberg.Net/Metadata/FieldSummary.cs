namespace Iceberg.Net.Metadata;

public readonly record struct FieldSummary(
    bool? ContainsNan,
    bool ContainsNull,
    byte[]? LowerBound,
    byte[]? UpperBound);
